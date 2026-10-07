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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.streaming.Utils;
import org.wso2.carbon.inbound.streaming.VFSConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Appends the raw bytes of recoverable, per-record streaming failures to a sidecar file in a
 * configured folder, one file per source file. The sidecar name inserts a caller-supplied marker
 * before the source extension (e.g. marker {@code "parse.fail"} turns {@code input.jsonl} into
 * {@code input.parse.fail.jsonl}; marker {@code "mediation.fail"} into
 * {@code input.mediation.fail.jsonl}) so that whole-file failures (which keep the original name),
 * parse-error sidecars and mediation-error sidecars can all share the same fault folder without
 * colliding.
 * <p>
 * The destination is created lazily on the first {@link #append(byte[])} call, so a source file
 * with no failed records produces no sidecar. Bytes are written verbatim followed by a single
 * newline (raw-line-only format), so a JSONL sidecar is itself re-processable.
 * <p>
 * If the destination cannot be opened, the writer degrades to a no-op (failures are still counted
 * and logged by the caller); it never throws out of {@link #append(byte[])} for a write failure so
 * that streaming can continue.
 */
public class FailedRecordWriter implements Closeable {

    private static final Log log = LogFactory.getLog(FailedRecordWriter.class);
    private static final byte[] NEWLINE = System.lineSeparator().getBytes();

    private final FileSystemManager fsManager;
    private final VFSConfig vfsConfig;
    private final String folderUri;
    private final String sourceBaseName;
    private final String marker;

    private boolean initialized;
    private FileObject destFile;
    private OutputStream out;
    private long written;

    /**
     * @param fsManager      the VFS manager used to resolve the destination
     * @param vfsConfig      the inbound VFS configuration (for scheme options)
     * @param folderUri      VFS URI of the destination folder (must be non-empty)
     * @param sourceBaseName base name of the source file
     * @param marker         marker inserted before the source extension to form the sidecar name,
     *                       e.g. {@code "fail"} gives {@code input.fail.jsonl} and
     *                       {@code "mediation.fail"} gives {@code input.mediation.fail.jsonl}
     */
    public FailedRecordWriter(FileSystemManager fsManager, VFSConfig vfsConfig, String folderUri,
                              String sourceBaseName, String marker) {
        this.fsManager = fsManager;
        this.vfsConfig = vfsConfig;
        this.folderUri = folderUri;
        this.sourceBaseName = sourceBaseName;
        this.marker = marker;
    }

    /**
     * Derive the sidecar file name from the source base name by inserting {@code .<marker>} before
     * the extension: {@code toFailedRecordsFileName("input.jsonl", "fail") -> input.fail.jsonl},
     * {@code toFailedRecordsFileName("data", "fail") -> data.fail}.
     */
    static String toFailedRecordsFileName(String baseName, String marker) {
        int dot = baseName.lastIndexOf('.');
        if (dot > 0) {
            return baseName.substring(0, dot) + "." + marker + baseName.substring(dot);
        }
        return baseName + "." + marker;
    }

    /**
     * Append the raw bytes of a failed record to the sidecar, opening it on first use. Never throws
     * on a write failure - it logs and degrades to a no-op so streaming can continue.
     */
    public void append(byte[] rawContent) {
        if (rawContent == null) {
            return;
        }
        ensureOpen();
        if (out == null) {
            return;
        }
        try {
            out.write(rawContent);
            out.write(NEWLINE);
            out.flush();
            written++;
        } catch (IOException e) {
            log.error("Failed to append a failed record to '" + folderUri + "/" + sourceBaseName
                    + "'. Subsequent failed records for this file will not be written.", e);
            closeQuietly();
            out = null;
        }
    }

    private void ensureOpen() {
        if (initialized) {
            return;
        }
        initialized = true;
        try {
            Map<String, String> query = Utils.parseSchemeFileOptions(folderUri, new Properties());
            if (query != null) {
                query.putAll(vfsConfig.getVfsSchemeProperties());
            }
            String location = Utils.extractPath(Utils.stripVfsSchemeIfPresent(folderUri));
            FileSystemOptions fso = Utils.attachFileSystemOptions(query, fsManager);
            FileObject folder = fsManager.resolveFile(location, fso);
            folder.createFolder();
            destFile = folder.resolveFile(toFailedRecordsFileName(sourceBaseName, marker));
            // Append so repeated runs / multiple chunks accumulate into one sidecar.
            out = destFile.getContent().getOutputStream(true);
            if (log.isDebugEnabled()) {
                log.debug("Opened failed-records sidecar '" + destFile.getName().getBaseName()
                        + "' in '" + location + "'");
            }
        } catch (Exception e) {
            log.error("Could not open failed-records sidecar for '" + sourceBaseName + "' in folder '"
                    + folderUri + "'. Failed records will be logged only.", e);
            closeQuietly();
            out = null;
        }
    }

    /**
     * @return the number of failed records written to the sidecar so far
     */
    public long getWrittenCount() {
        return written;
    }

    /**
     * Delete the sidecar and its contents. Used when the file is ultimately treated as a complete
     * failure (e.g. the max-failed-records threshold is exceeded) and the whole source file will be
     * moved to the fault folder, making the partial sidecar redundant.
     */
    public void discard() {
        closeQuietly();
        out = null;
        try {
            if (destFile != null && destFile.exists()) {
                destFile.delete();
                if (log.isDebugEnabled()) {
                    log.debug("Discarded partial failed-records sidecar '"
                            + destFile.getName().getBaseName() + "'");
                }
            }
        } catch (Exception e) {
            log.warn("Could not discard partial failed-records sidecar for '" + sourceBaseName + "'", e);
        }
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                log.debug("Error closing failed-records sidecar stream", e);
            }
        }
        if (destFile != null) {
            try {
                destFile.close();
            } catch (Exception e) {
                log.debug("Error closing failed-records sidecar file object", e);
            }
        }
    }
}
