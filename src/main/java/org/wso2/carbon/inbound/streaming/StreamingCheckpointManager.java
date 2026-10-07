/*
 *  Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.streaming;

import org.apache.axiom.om.OMNode;
import org.apache.axiom.om.OMText;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.registry.Registry;
import org.wso2.carbon.inbound.streaming.VFSConfig;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;

import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.CRC32;

/**
 * Reads and writes a streamed file's {@link StreamingCheckpoint} in the MI registry so processing
 * can resume after a restart. Checkpoints live under
 * {@code gov:/fileStreamingCheckpoints/{inboundName}/<fileKey>.json}.
 * <p>
 * Writes are not atomic in the registry, so the previous checkpoint is kept as a {@code .bak}: a
 * crash mid-write leaves at most a one-interval-old but valid checkpoint to fall back to.
 */
public class StreamingCheckpointManager {

    private static final Log log = LogFactory.getLog(StreamingCheckpointManager.class);
    private static final String JSON_MEDIA_TYPE = "application/json";
    private static final String BAK_SUFFIX = ".bak";

    private final Registry registry;
    private final VFSConfig config;
    private final String inboundName;
    // Raw URI. Used ONLY to derive the checkpoint's registry path, which has to stay stable for
    // the same source file, and to compute the stored hash. Never persisted: for sftp/smb it
    // carries credentials, and the registry is a shared mount.
    private final String fileUri;
    // The same URI with any password replaced. For log lines only, never for the checkpoint.
    private final String maskedFileUri;
    private final String path;
    private final StreamingCheckpoint.Fingerprint fingerprint;
    private final String configHash;

    /**
     * @throws Exception if the file fingerprint cannot be computed (the caller should then proceed
     *                   without checkpointing)
     */
    public StreamingCheckpointManager(SynapseEnvironment synapseEnvironment, VFSConfig config,
                                      String inboundName, FileObject file) throws Exception {
        this.registry = synapseEnvironment.getSynapseConfiguration().getRegistry();
        if (this.registry == null) {
            throw new IllegalStateException("No registry available for streaming checkpoints");
        }
        this.config = config;
        this.inboundName = inboundName;
        this.fileUri = file.getName().getURI();
        this.maskedFileUri = Utils.maskURLPassword(this.fileUri);
        this.fingerprint = computeFingerprint(file);
        this.configHash = computeConfigHash(config);
        this.path = resolvePath();
    }

    /**
     * Load the stored checkpoint if it exists and is safe to resume for this file/config (matching
     * fingerprint, config, mode and format); otherwise {@code null} (start fresh).
     */
    public StreamingCheckpoint loadResumable() {
        boolean fromBackup = false;
        StreamingCheckpoint cp = StreamingCheckpoint.fromJson(readRaw(path));
        if (cp == null) {
            cp = StreamingCheckpoint.fromJson(readRaw(path + BAK_SUFFIX));
            fromBackup = cp != null;
        }
        if (cp == null) {
            // Not an error: the usual case is a file nothing has started yet. Logged so a resume
            // that was expected can be told apart from one that was refused.
            log.info("No streaming checkpoint at " + path + " for " + maskedFileUri
                    + "; this file will be streamed from its first record.");
            return null;
        }
        String reason = cp.whyNotResumableFor(fingerprint, configHash,
                config.getStreamingMode(), config.getStreamingInputFormat());
        if (reason != null) {
            log.warn("Ignoring the streaming checkpoint at " + path
                    + (fromBackup ? " (backup copy)" : "") + " for " + maskedFileUri
                    + " because " + reason + ". It recorded " + cp.getRecordsConsumed()
                    + " consumed records; the file will be streamed from its first record instead.");
            return null;
        }
        if (fromBackup) {
            log.warn("The primary streaming checkpoint at " + path + " was unreadable; resuming "
                    + maskedFileUri + " from the backup copy.");
        }
        return cp;
    }

    /**
     * Persist the current progress. Keeps the previous checkpoint as a {@code .bak} before
     * overwriting the primary. Never throws - a checkpoint write failure must not fail the stream.
     */
    public void save(long recordsConsumed, long chunksConsumed, long processed, long parseFailed,
                     long mediationFailed, long now) {
        try {
            StreamingCheckpoint cp = new StreamingCheckpoint();
            cp.setSchemaVersion(StreamingConstants.CHECKPOINT_SCHEMA_VERSION);
            cp.setInboundName(inboundName);
            cp.setFileUriHash(sha256Hex(fileUri));
            cp.setFileFingerprint(fingerprint);
            cp.setStreamingMode(config.getStreamingMode());
            cp.setInputFormat(config.getStreamingInputFormat());
            cp.setConfigHash(configHash);
            cp.setRecordsConsumed(recordsConsumed);
            cp.setChunksConsumed(chunksConsumed);
            cp.setProcessedRecords(processed);
            cp.setParseFailedRecords(parseFailed);
            cp.setMediationFailedRecords(mediationFailed);
            cp.setUpdatedAt(now);

            String previous = readRaw(path);
            if (previous != null) {
                write(path + BAK_SUFFIX, previous);
            }
            write(path, cp.toJson());
        } catch (Exception e) {
            log.warn("Failed to write streaming checkpoint for " + maskedFileUri
                    + "; resume may restart this file from the beginning.", e);
        }
    }

    /** Remove the checkpoint (and its backup). Called on successful completion or whole-file failure. */
    public void clear() {
        safeDelete(path);
        safeDelete(path + BAK_SUFFIX);
    }

    // ----- internals -----

    private void write(String targetPath, String json) {
        // isResourceExists rather than a full read: updateResource only writes a resource that is
        // already there, so we only need to know whether it exists. Reading the whole resource back
        // just to make that decision costs an extra round trip to the registry mount on every save.
        if (registry.isResourceExists(targetPath)) {
            registry.updateResource(targetPath, json);   // updateResource only writes an existing file
        } else {
            registry.newNonEmptyResource(targetPath, false, JSON_MEDIA_TYPE, json, null);
        }
    }

    /** Read a registry resource's text, or {@code null} if absent/unreadable. */
    private String readRaw(String key) {
        try {
            OMNode node = registry.lookup(key);
            if (node instanceof OMText) {
                return decodeContent(((OMText) node).getText());
            }
        } catch (Exception e) {
            // Absent or unreadable - treat as no checkpoint.
            if (log.isDebugEnabled()) {
                log.debug("No readable registry resource at " + key + ": " + e.getMessage());
            }
        }
        return null;
    }

    /**
     * The MI registry returns a non-text resource (media type {@code application/json}) base64
     * encoded, so decode it when the content is valid base64; JSON text (which starts with '{',
     * not a base64 character) is returned verbatim.
     */
    private static String decodeContent(String content) {
        if (content == null || content.isEmpty()) {
            return content;
        }
        try {
            byte[] decoded = Base64.getDecoder().decode(content.trim());
            return new String(decoded, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException notBase64) {
            return content;
        }
    }

    private void safeDelete(String key) {
        try {
            if (registry.isResourceExists(key)) {
                registry.delete(key);
            }
        } catch (Exception e) {
            log.warn("Failed to delete streaming checkpoint resource " + key, e);
        }
    }

    private String resolvePath() {
        return StreamingConstants.CHECKPOINT_REGISTRY_ROOT + "/" + sanitize(inboundName) + "/"
                + crc32Hex(fileUri) + ".json";
    }

    private StreamingCheckpoint.Fingerprint computeFingerprint(FileObject file) throws Exception {
        FileContent content = file.getContent();
        long size = content.getSize();
        long lastModified = content.getLastModifiedTime();
        CRC32 crc = new CRC32();
        int budget = StreamingConstants.CHECKPOINT_FINGERPRINT_BYTES;
        try (InputStream in = content.getInputStream()) {
            byte[] buf = new byte[8192];
            int read;
            while (budget > 0 && (read = in.read(buf, 0, Math.min(buf.length, budget))) != -1) {
                crc.update(buf, 0, read);
                budget -= read;
            }
        }
        return new StreamingCheckpoint.Fingerprint(size, lastModified,
                StreamingConstants.CHECKPOINT_FINGERPRINT_BYTES,
                StreamingConstants.CHECKPOINT_HASH_ALGORITHM, Long.toHexString(crc.getValue()));
    }

    /** Hash of every config parameter that affects record identity/numbering/parsing. */
    private String computeConfigHash(VFSConfig config) {
        String canonical = config.getStreamingInputFormat() + "|"
                + config.getStreamingMode() + "|"
                + config.getStreamingChunkSize() + "|"
                + config.getStreamingCharset() + "|"
                + config.getStreamingCsvDelimiter() + "|"
                + config.getStreamingCsvQuote() + "|"
                + config.isStreamingCsvHasHeader() + "|"
                + config.isStreamingAddHeadersToEachResult() + "|"
                + config.getStreamingJsonPath() + "|"
                + config.getStreamingOutputProperty() + "|"
                + config.getStreamingCsvDataTypesRaw();
        return crc32Hex(canonical);
    }

    /**
     * One-way identity for the source file. SHA-256 rather than the CRC32 used for the registry
     * path: the path only has to be stable and short, whereas this stands in for the URI itself
     * and must not be guessable back to a host, a user or a password.
     */
    private static String sha256Hex(String value) {
        try {
            MessageDigest digest =
                MessageDigest.getInstance(StreamingConstants.CHECKPOINT_URI_HASH_ALGORITHM);
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the JRE; if it is somehow absent, store nothing rather than
            // falling back to anything that could expose the URI.
            log.warn("SHA-256 unavailable; the checkpoint will not record a file identity.", e);
            return "";
        }
    }

    private static String crc32Hex(String value) {
        CRC32 crc = new CRC32();
        crc.update(value.getBytes(StandardCharsets.UTF_8));
        return Long.toHexString(crc.getValue());
    }

    /** Keep only characters safe for a registry path segment. */
    private static String sanitize(String name) {
        if (name == null || name.isEmpty()) {
            return "unnamed";
        }
        return name.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}
