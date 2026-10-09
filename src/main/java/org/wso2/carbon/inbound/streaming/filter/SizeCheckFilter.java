/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
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

package org.wso2.carbon.inbound.streaming.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.streaming.Utils;
import org.wso2.carbon.inbound.streaming.VFSConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;

/**
 * Filter that holds back a file while it may still be being written: its size and last-modified
 * time are read, then read again {@code transport.vfs.CheckSizeInterval} ms later, and the file is
 * only accepted if neither changed. With {@code transport.vfs.CheckSizeIgnoreEmpty} an empty file
 * is held back too.
 * <p>
 * Only metadata is read. An earlier version hashed the whole file twice instead, which for a
 * multi-GB file meant reading it in full twice before processing started - over SFTP/SMB, two full
 * downloads.
 */
public class SizeCheckFilter implements Filter {
    Log log = LogFactory.getLog(SizeCheckFilter.class.getName());
    VFSConfig vfsConfig;
    FileSystemManager fsManager;

    public SizeCheckFilter(VFSConfig vfsConfig, FileSystemManager fsManager) {
        this.vfsConfig = vfsConfig;
        this.fsManager = fsManager;
    }

    @Override
    public boolean accept(FileObject fileObject) {
        try {
            return !isFileStillUploading(fileObject);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * @return true if the file is empty (when empty files are to be skipped) or still changing
     */
    private boolean isFileStillUploading(FileObject child) {
        try {
            Snapshot before = Snapshot.of(child);
            if (vfsConfig.getCheckSizeIgnoreEmpty() && before.size == 0) {
                log.debug("Skipping empty file: " + Utils.maskURLPassword(child.getName().toString()));
                return true;
            }
            return isFileStillChanging(child, before);
        } catch (Exception e) {
            log.debug("Could not read the size of "
                + Utils.maskURLPassword(child.getName().toString()) + "; skipping it this poll.", e);
            return true;
        }
    }

    /**
     * Wait checkSizeInterval ms and read the size and last-modified time again. If either moved,
     * the file is still being written.
     */
    private boolean isFileStillChanging(FileObject child, Snapshot before) throws Exception {
        long checkSizeInterval = vfsConfig.getCheckSizeInterval();
        log.debug("Check if file is still uploading. Now sleep " + checkSizeInterval + " ms");
        Thread.sleep(checkSizeInterval);
        // VFS caches file attributes; drop them so the second reading is a fresh one.
        child.refresh();
        Snapshot after = Snapshot.of(child);
        if (!before.equals(after)) {
            log.debug("File is still uploading: " + Utils.maskURLPassword(child.getName().toString())
                + " " + before + " -> " + after);
            return true;
        }
        return false;
    }

    /** A file's size and last-modified time at one moment. */
    private static final class Snapshot {

        // Some providers do not report a modification time; then the size alone decides.
        private static final long UNKNOWN = Long.MIN_VALUE;

        private final long size;
        private final long lastModified;

        private Snapshot(long size, long lastModified) {
            this.size = size;
            this.lastModified = lastModified;
        }

        static Snapshot of(FileObject file) throws FileSystemException {
            long size = file.getContent().getSize();
            long lastModified;
            try {
                lastModified = file.getContent().getLastModifiedTime();
            } catch (FileSystemException e) {
                lastModified = UNKNOWN;
            }
            return new Snapshot(size, lastModified);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Snapshot)) {
                return false;
            }
            Snapshot other = (Snapshot) o;
            boolean timesComparable = lastModified != UNKNOWN && other.lastModified != UNKNOWN;
            return size == other.size && (!timesComparable || lastModified == other.lastModified);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(size);
        }

        @Override
        public String toString() {
            return "{size=" + size + ", lastModified=" + lastModified + "}";
        }
    }
}
