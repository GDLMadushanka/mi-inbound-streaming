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
import org.apache.commons.vfs2.FileSystemManager;

import java.io.InputStream;
import java.security.MessageDigest;

/**
 * Filter to filter files based on file size checking.
 */
public class SizeCheckFilter implements Filter {
    public static final String EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e";
    public static final String MD5 = "MD5";
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


    private boolean isFileStillUploading(FileObject child) {
        if (vfsConfig.getCheckSizeIgnoreEmpty() && vfsConfig.getCheckSizeInterval() > 0) {
            //CheckEmpty and CheckSize are not active - return false (file is not uploading)
            return false;
        }
        InputStream inputStream = null;
        try {
            //get first MD5
            log.debug("Create MD5 Checksum of File: " + Utils.maskURLPassword(child.getName().toString()));
            inputStream = child.getContent().getInputStream();
            String md5 = getMD5Checksum(inputStream);
            return isFileEmpty(md5) || isFileStillChangingSize(child, md5);
        } catch (Exception e) {
            return true;
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * This Function calculates the MD5 Hash of the FileObject, waits
     * checkSizeInterval [ms] time, and calculates the MD5 Hash again. If they
     * are the same, the file is finished uploading and can be consumed.
     *
     * @param child the fileobject currently read
     * @return if file is empty or the filesize is changing
     */
    private boolean isFileStillChangingSize(FileObject child, String md5) {
        try {
            //get interval time
            long checkSizeInterval = vfsConfig.getCheckSizeInterval();

            //wait interval time
            log.debug("Check if file is still uploading. Now sleep " + checkSizeInterval + " ms");
            Thread.sleep(checkSizeInterval);
            String md5AfterSleep = getMD5Checksum(child);
            if (!md5.equals(md5AfterSleep)) {
                //file is still uploading
                log.debug("File is still uploading. md5 Hashcode Before=" + md5 + " After=" + md5AfterSleep);
                return true;
            }
        } catch (Exception e) {
            return true;
        }
        return false;
    }

    private String getMD5Checksum(FileObject child) {
        try (InputStream inputStream = child.getContent().getInputStream()) {
            //get first MD5
            return getMD5Checksum(inputStream);
        } catch (Exception e) {
            log.error("Error while calculating MD5 checksum for file: " +
                    Utils.maskURLPassword(child.getName().toString()), e);
            return null;
        }
    }

    /**
     * Return the MD5Checksum of a InputStream as String
     *
     * @param fis inputStream of the current file
     * @return MD5 Checksum
     * @throws Exception
     */
    private String getMD5Checksum(InputStream fis) throws Exception {
        byte[] b = createChecksum(fis);
        String result = "";
        for (int i = 0; i < b.length; i++) {
            result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
        }
        return result;
    }

    /**
     * Used by getMD5Checksum to get the MD5 as byte array
     *
     * @param fis inputStream of the current file
     * @return MD5 Checksum
     * @throws Exception
     */
    private byte[] createChecksum(InputStream fis) throws Exception {
        byte[] buffer = new byte[1024];
        MessageDigest complete = MessageDigest.getInstance(MD5);
        int numRead;
        do {
            numRead = fis.read(buffer);
            if (numRead > 0) {
                complete.update(buffer, 0, numRead);
            }
        } while (numRead != -1);
        return complete.digest();
    }

    /**
     * Verifies if the given md5 is the md5 of an Empty File
     *
     * @param md5 of the given file
     * @return true if configuration is set and file is empty
     */
    private boolean isFileEmpty(String md5) {
        if (vfsConfig.getCheckSizeIgnoreEmpty()) {
            return EMPTY_MD5.equals(md5);
        }
        return false;
    }

}
