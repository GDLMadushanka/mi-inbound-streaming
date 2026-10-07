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

package org.wso2.carbon.inbound.streaming.lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.streaming.Utils;
import org.wso2.carbon.inbound.streaming.VFSConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import static org.wso2.carbon.inbound.streaming.Utils.maskURLPassword;
import static org.wso2.carbon.inbound.streaming.VFSConstants.LOCK_FILE_SUFFIX;
import static org.wso2.carbon.inbound.streaming.VFSConstants.STR_SPLITER;


public class LockManager {
    private static final Log log = LogFactory.getLog(LockManager.class);
    private static final Random randomNumberGenerator = new Random();
    private final FileSystemManager fsManager;
    private final FileSystemOptions fso;
    private boolean fileLock;
    private boolean autoLockRelease;
    private boolean autoLockReleaseSameNode;
    private long autoLockReleaseInterval;

    public LockManager(boolean fileLock, VFSConfig vfsConfig,
                       FileSystemManager fsManager,
                       FileSystemOptions fso) {
        this.fileLock = fileLock;
        initializeFileLockingParams(vfsConfig);
        this.fsManager = fsManager;
        this.fso = fso;
    }

    /**
     * Acquire lock on file if required
     */
    public boolean acquireLock(FileObject fileObject) {
        if (!fileLock) {
            return true;
        }

        String strContext = fileObject.getName().getURI();

        // When processing a directory list is fetched initially. Therefore
        // there is still a chance of file processed by another process.
        // Need to check the source file before processing.
        try {
            String parentURI = fileObject.getParent().getName().getURI();
            if (parentURI.contains("?")) {
                String suffix = parentURI.substring(parentURI.indexOf("?"));
                strContext += suffix;
            }
            FileObject sourceFile = fsManager.resolveFile(strContext, fso);
            if (!sourceFile.exists()) {
                return false;
            }
        } catch (FileSystemException e) {
            return false;
        }

        return acquireLock(fsManager, fileObject, fso, true);
    }


    /**
     * Acquires a file item lock before processing the item, guaranteeing that
     * the file is not processed while it is being uploaded and/or the item is
     * not processed by two listeners
     *
     * @param fsManager used to resolve the processing file
     * @param fo        representing the processing file item
     * @param fso       represents file system options used when resolving file from file system manager.
     * @return boolean true if the lock has been acquired or false if not
     */
    public synchronized boolean acquireLock(FileSystemManager fsManager, FileObject fo,
                                            FileSystemOptions fso, boolean isListener) {
        String strLockValue = getLockValue();
        byte[] lockValue = strLockValue.getBytes();
        FileObject lockObject = null;
        String fullPath = Utils.getFullPath(fo);

        try {
            // check whether there is an existing lock for this item, if so it is assumed
            // to be processed by an another listener (downloading) or a sender (uploading)
            // lock file is derived by attaching the ".lock" second extension to the file name
            lockObject = fsManager.resolveFile(fullPath + LOCK_FILE_SUFFIX, fso);
            if (lockObject.exists()) {
                log.debug("There seems to be an external lock, aborting the processing of the file "
                        + maskURLPassword(fo.getName().getURI())
                        + ". This could possibly be due to some other party already "
                        + "processing this file or the file is still being uploaded");
                if (autoLockRelease) {
                    releaseLock(lockValue, strLockValue, lockObject, autoLockReleaseSameNode,
                            autoLockReleaseInterval);
                }
            } else {
                if (isListener) {
                    //Check the original file existence before the lock file to handle concurrent access scenario
                    FileObject originalFileObject = fsManager.resolveFile(fullPath, fso);
                    if (!originalFileObject.exists()) {
                        return false;
                    }
                }
                if (!createLockFile(lockValue, lockObject, fullPath)) {
                    return false;
                }

                // check whether the lock is in place and is it me who holds the lock. This is
                // required because it is possible to write the lock file simultaneously by
                // two processing parties. It checks whether the lock file content is the same
                // as the written random lock value.
                // NOTE: this may not be optimal but is sub optimal
                FileObject verifyingLockObject = fsManager.resolveFile(
                        fullPath + LOCK_FILE_SUFFIX, fso);
                if (verifyingLockObject.exists() && verifyLock(lockValue, verifyingLockObject)) {
                    return true;
                }
            }
        } catch (FileSystemException fse) {
            log.error("Cannot get the lock for the file : " + maskURLPassword(fo.getName().getURI()) + " before processing", fse);
            if (lockObject != null) {
                try {
                    fsManager.closeFileSystem(lockObject.getParent().getFileSystem());
                } catch (FileSystemException e) {
                    log.warn("Unable to close the lockObject parent file system");
                }
            } else {
                try {
                    ((DefaultFileSystemManager) fsManager).closeCachedFileSystem(fullPath + LOCK_FILE_SUFFIX, fso);
                } catch (Exception e1) {
                    log.warn("Unable to clear file system", e1);
                }
            }
        }
        return false;
    }


    /**
     * Generate a random lock value to ensure that there are no two parties processing the same file
     * Lock format random:hostname:hostip:time
     *
     * @return lock value as a string
     */
    private String getLockValue() {

        StringBuilder lockValueBuilder = new StringBuilder();
        lockValueBuilder.append(randomNumberGenerator.nextLong());
        try {
            lockValueBuilder.append(STR_SPLITER)
                    .append(InetAddress.getLocalHost().getHostName())
                    .append(STR_SPLITER)
                    .append(InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException ue) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to get the Hostname or IP.", ue);
            }
        }
        lockValueBuilder.append(STR_SPLITER).append((new Date()).getTime());
        return lockValueBuilder.toString();
    }

    private void releaseLock(byte[] bLockValue, String sLockValue, FileObject lockObject,
                             Boolean autoLockReleaseSameNode, Long autoLockReleaseInterval) {
        try {
            // Read the WHOLE lock file. Sizing the buffer to the new lock value instead (as this
            // did) only works when the stored value happens to be the same length - and it does
            // not, because the leading random number varies in digits. A short stored value left
            // trailing NULs that broke the timestamp parse; a long one was silently truncated into
            // a bogus timestamp. Either way the age came out wrong, so the configured release
            // interval was not honoured and two nodes could hold the same file at once.
            String strVal = readLockValue(lockObject);
            if (strVal == null) {
                lockObject.close();
                return;
            }
            // Lock format random:hostname:hostip:time
            String[] arrVal = strVal.split(STR_SPLITER);
            String[] arrValNew = sLockValue.split(STR_SPLITER);
            if (arrVal.length < 4 || arrValNew.length < 4) {
                // Not a lock this code wrote; leave it alone rather than guess.
                log.warn("Unrecognised lock file content; leaving the lock in place.");
                lockObject.close();
                return;
            }
            if (canReleaseLock(arrVal, arrValNew)) {
                Long lInterval = lockAgeMillis(arrValNew[3], arrVal[3]);
                if (lInterval == null) {
                    // The age is unknown, so we cannot tell a dead lock from a live one. Keeping
                    // it is the safe choice: a stuck file is recoverable, double processing is not.
                    log.warn("Could not determine the age of the lock file; leaving it in place.");
                    lockObject.close();
                    return;
                }
                deleteLockFile(lockObject, autoLockReleaseInterval, lInterval);
            } else {
                lockObject.close();
            }
        } catch (IOException e) {
            log.error("Couldn't release the lock for auto-release of lock file");
        }
    }


    /** Read a lock file whole, so the value is never truncated or NUL-padded. */
    private String readLockValue(FileObject lockObject) {
        try (InputStream is = lockObject.getContent().getInputStream();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buf = new byte[256];
            int read;
            while ((read = is.read(buf)) != -1) {
                out.write(buf, 0, read);
            }
            String value = new String(out.toByteArray(), StandardCharsets.UTF_8).trim();
            return value.isEmpty() ? null : value;
        } catch (IOException e) {
            log.warn("Could not read the lock file to decide on auto-release.", e);
            return null;
        }
    }

    /** Age of the existing lock in millis, or null when either timestamp will not parse. */
    private Long lockAgeMillis(String nowToken, String lockedAtToken) {
        try {
            return Long.parseLong(nowToken.trim()) - Long.parseLong(lockedAtToken.trim());
        } catch (NumberFormatException nfe) {
            log.debug("Lock file timestamp is not a number: '" + lockedAtToken + "'", nfe);
            return null;
        }
    }

    private boolean createLockFile(byte[] lockValue, FileObject lockObject, String fullPath)
            throws FileSystemException {
        // write a lock file before starting of the processing, to ensure that the
        // item is not processed by any other parties
        lockObject.createFile();
        OutputStream stream = lockObject.getContent().getOutputStream();
        try {
            stream.write(lockValue);
            stream.flush();
        } catch (IOException e) {
            lockObject.delete();
            log.error("Couldn't create the lock file before processing the file "
                    + maskURLPassword(fullPath), e);
            return false;
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                log.debug("Error closing stream", e);
            }
            lockObject.close();
        }
        return true;
    }

    private void deleteLockFile(FileObject lockObject, Long autoLockReleaseInterval, long lInterval)
            throws FileSystemException {
        if (autoLockReleaseInterval == null || autoLockReleaseInterval <= lInterval) {
            try {
                lockObject.delete();
            } catch (Exception e) {
                log.warn("Unable to delete the lock file during auto release cycle.", e);
            } finally {
                lockObject.close();
            }
        }
    }


    private boolean verifyLock(byte[] lockValue, FileObject lockObject) {
        try {
            InputStream is = lockObject.getContent().getInputStream();
            byte[] val = new byte[lockValue.length];
            // noinspection ResultOfMethodCallIgnored
            is.read(val);
            if (Arrays.equals(lockValue, val) && is.read() == -1) {
                return true;
            } else {
                log.debug("The lock has been acquired by an another party");
            }
        } catch (IOException e) {
            log.error("Couldn't verify the lock", e);
            return false;
        }
        return false;
    }


    /**
     * Initialize file locking related parameters from properties
     */
    private void initializeFileLockingParams(VFSConfig vfsConfig) {
        // Check if file locking is enabled
        this.fileLock = vfsConfig.isFileLocking();
        // Auto lock release configuration
        this.autoLockRelease = vfsConfig.isAutoLockRelease();
        if (autoLockRelease) {
            autoLockReleaseInterval = vfsConfig.getAutoLockReleaseInterval();
        }
        this.autoLockReleaseSameNode = vfsConfig.getAutoLockReleaseSameNode();
    }

    private boolean canReleaseLock(String[] oldLock, String[] newLock) {
        if (!autoLockReleaseSameNode) {
            return true;
        }

        return oldLock.length == 4 && newLock.length == 4 && oldLock[1].equals(newLock[1]) && oldLock[2].equals(newLock[2]);
    }

    /**
     * Release a file item lock acquired either by the VFS listener or a sender
     *
     * @param fo representing the processed file
     */
    public void releaseLock(FileObject fo) {
        String fullPath = fo.getName().getURI();
        FileObject lockObject;

        try {
            int pos = fullPath.indexOf('?');
            if (pos > -1) {
                fullPath = fullPath.substring(0, pos);
            }
            lockObject = fsManager.resolveFile(fullPath + LOCK_FILE_SUFFIX, fso);
            if (lockObject.exists()) {
                lockObject.delete();
            }
        } catch (FileSystemException e) {
            log.error("Couldn't release the lock for the file : "
                    + maskURLPassword(fo.getName().getURI()) + " after processing");
            try {
                ((DefaultFileSystemManager) fsManager).closeCachedFileSystem(fullPath + LOCK_FILE_SUFFIX, fso);
            } catch (Exception e1) {
                log.warn("Unable to clear file system", e1);
            }
        }
    }

}
