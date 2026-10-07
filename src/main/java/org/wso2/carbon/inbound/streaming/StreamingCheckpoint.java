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

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * The persisted state that lets a streamed file resume after a restart. Serialized as compact JSON
 * to the registry (see {@link StreamingCheckpointManager}).
 * <p>
 * {@link #recordsConsumed} is the authoritative resume pointer - the record number of the last
 * fully-handled record/chunk. On resume the processor skips that many records. The three counts are
 * metrics, with the invariant {@code processedRecords + parseFailedRecords + mediationFailedRecords
 * == recordsConsumed}.
 */
public class StreamingCheckpoint {

    private static final Gson GSON = new Gson();

    private int schemaVersion;
    private String inboundName;
    // SHA-256 of the source file's URI, never the URI itself. A checkpoint is machine state, not
    // something an operator reads, and on a shared registry mount a remote URI would carry
    // credentials. The log lines name the file; this only has to identify it.
    private String fileUriHash;
    private Fingerprint fileFingerprint;
    private String streamingMode;
    private String inputFormat;
    private String configHash;
    private long recordsConsumed;
    // Chunks emitted so far, so CHUNK mode keeps numbering them from where it left off instead of
    // restarting at 1 after a resume. Zero in RECORD mode, and zero for a checkpoint written by a
    // build that predates this field - numbering then behaves as it did before.
    private long chunksConsumed;
    private long processedRecords;
    private long parseFailedRecords;
    private long mediationFailedRecords;
    private long updatedAt;

    /** A cheap file identity: size + last-modified + a hash of the leading bytes. */
    public static class Fingerprint {
        private long size;
        private long lastModified;
        private int hashedBytes;
        private String algorithm;
        private String hash;

        public Fingerprint() {
        }

        public Fingerprint(long size, long lastModified, int hashedBytes, String algorithm, String hash) {
            this.size = size;
            this.lastModified = lastModified;
            this.hashedBytes = hashedBytes;
            this.algorithm = algorithm;
            this.hash = hash;
        }

        public long getSize() {
            return size;
        }

        public long getLastModified() {
            return lastModified;
        }

        public int getHashedBytes() {
            return hashedBytes;
        }

        public String getAlgorithm() {
            return algorithm;
        }

        public String getHash() {
            return hash;
        }

        /** True when both fingerprints refer to the same file content (size, mtime and sample hash). */
        /**
         * Whether both fingerprints identify the same file content.
         * <p>
         * Deliberately ignores {@link #lastModified}. A modification time is not a reliable
         * identity across the nodes of a cluster: the same local file has been observed reported
         * as {@code ...539594} by one node and {@code ...539000} by another, the second truncated
         * to a whole second, because the precision depends on the file system and on how the
         * {@code FileObject} was resolved. Comparing it exactly made a node refuse a checkpoint
         * another node had just written and re-read the whole file from its first record. It also
         * made a file that was moved away and copied back - identical bytes, new timestamp - look
         * like a different file.
         * <p>
         * Identity is therefore the content: the byte count plus a hash of the leading
         * {@link #hashedBytes} bytes. The timestamp is still recorded, and still reported by
         * {@link #describeDifference}, purely as diagnostic detail.
         */
        public boolean matches(Fingerprint other) {
            return other != null
                    && size == other.size
                    && hashedBytes == other.hashedBytes
                    && (algorithm == null
                        ? other.algorithm == null : algorithm.equals(other.algorithm))
                    && (hash == null ? other.hash == null : hash.equals(other.hash));
        }

        /** Which fields differ, for the log line explaining a refused resume. */
        public String describeDifference(Fingerprint other) {
            if (other == null) {
                return "no current fingerprint";
            }
            StringBuilder sb = new StringBuilder();
            if (size != other.size) {
                sb.append("size ").append(size).append("->").append(other.size).append(' ');
            }
            if (hashedBytes != other.hashedBytes) {
                sb.append("hashedBytes ").append(hashedBytes).append("->")
                  .append(other.hashedBytes).append(' ');
            }
            if (algorithm == null ? other.algorithm != null : !algorithm.equals(other.algorithm)) {
                sb.append("algorithm ").append(algorithm).append("->")
                  .append(other.algorithm).append(' ');
            }
            if (hash == null ? other.hash != null : !hash.equals(other.hash)) {
                sb.append("first-").append(hashedBytes).append("-byte ").append(algorithm)
                  .append(' ').append(hash).append("->").append(other.hash).append(' ');
            }
            if (sb.length() == 0) {
                return "no field differs";
            }
            // Timestamps are not part of the comparison, but a drift alongside a real difference
            // is worth seeing in the log.
            if (lastModified != other.lastModified) {
                sb.append("(lastModified ").append(lastModified).append("->")
                  .append(other.lastModified).append(", not compared) ");
            }
            return sb.toString().trim();
        }
    }

    public StreamingCheckpoint() {
    }

    /** @return the compact JSON form to persist. */
    public String toJson() {
        return GSON.toJson(this);
    }

    /** Parse a checkpoint from its JSON form, or {@code null} if the text is not a valid checkpoint. */
    public static StreamingCheckpoint fromJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            return GSON.fromJson(json, StreamingCheckpoint.class);
        } catch (JsonSyntaxException e) {
            return null;
        }
    }

    /**
     * True when this stored checkpoint can be safely resumed for the given file/config: same
     * fingerprint, same config, same mode and format.
     */
    public boolean isResumableFor(Fingerprint current, String configHash, String streamingMode,
                                  String inputFormat) {
        return whyNotResumableFor(current, configHash, streamingMode, inputFormat) == null;
    }

    /**
     * Why this checkpoint cannot be used to resume, or {@code null} if it can. Every rejection here
     * silently costs a full re-read of the source file, so the caller logs the reason: without it a
     * failed resume is indistinguishable from no checkpoint at all.
     */
    public String whyNotResumableFor(Fingerprint current, String configHash, String streamingMode,
                                     String inputFormat) {
        if (fileFingerprint == null) {
            return "the checkpoint carries no file fingerprint";
        }
        if (current == null) {
            return "the current file's fingerprint could not be computed";
        }
        if (!fileFingerprint.matches(current)) {
            return "the file changed since the checkpoint was written ("
                    + fileFingerprint.describeDifference(current) + ")";
        }
        if (this.configHash == null || !this.configHash.equals(configHash)) {
            return "the inbound's streaming configuration changed (checkpoint configHash="
                    + this.configHash + ", current=" + configHash + ")";
        }
        if (this.streamingMode == null || !this.streamingMode.equalsIgnoreCase(streamingMode)) {
            return "the streaming mode changed (checkpoint=" + this.streamingMode
                    + ", current=" + streamingMode + ")";
        }
        if (this.inputFormat == null || !this.inputFormat.equalsIgnoreCase(inputFormat)) {
            return "the input format changed (checkpoint=" + this.inputFormat
                    + ", current=" + inputFormat + ")";
        }
        return null;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getInboundName() {
        return inboundName;
    }

    public void setInboundName(String inboundName) {
        this.inboundName = inboundName;
    }

    public String getFileUriHash() {
        return fileUriHash;
    }

    public void setFileUriHash(String fileUriHash) {
        this.fileUriHash = fileUriHash;
    }

    public Fingerprint getFileFingerprint() {
        return fileFingerprint;
    }

    public void setFileFingerprint(Fingerprint fileFingerprint) {
        this.fileFingerprint = fileFingerprint;
    }

    public String getStreamingMode() {
        return streamingMode;
    }

    public void setStreamingMode(String streamingMode) {
        this.streamingMode = streamingMode;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    public String getConfigHash() {
        return configHash;
    }

    public void setConfigHash(String configHash) {
        this.configHash = configHash;
    }

    public long getChunksConsumed() {
        return chunksConsumed;
    }

    public void setChunksConsumed(long chunksConsumed) {
        this.chunksConsumed = chunksConsumed;
    }

    public long getRecordsConsumed() {
        return recordsConsumed;
    }

    public void setRecordsConsumed(long recordsConsumed) {
        this.recordsConsumed = recordsConsumed;
    }

    public long getProcessedRecords() {
        return processedRecords;
    }

    public void setProcessedRecords(long processedRecords) {
        this.processedRecords = processedRecords;
    }

    public long getParseFailedRecords() {
        return parseFailedRecords;
    }

    public void setParseFailedRecords(long parseFailedRecords) {
        this.parseFailedRecords = parseFailedRecords;
    }

    public long getMediationFailedRecords() {
        return mediationFailedRecords;
    }

    public void setMediationFailedRecords(long mediationFailedRecords) {
        this.mediationFailedRecords = mediationFailedRecords;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }
}
