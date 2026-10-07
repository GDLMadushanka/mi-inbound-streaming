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
import org.wso2.carbon.inbound.streaming.VFSConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

/**
 * Filter to filter files based on age.
 */
public class AgeFilter implements Filter {
    VFSConfig vfsConfig;
    private Log log = LogFactory.getLog(AgeFilter.class);

    public AgeFilter(VFSConfig vfsConfig) {
        this.vfsConfig = vfsConfig;
    }

    @Override
    public boolean accept(FileObject fileObject) {
        boolean isAccepted = false;
        if (vfsConfig.getMinimumAge() != null) {
            long age = 0;
            try {
                age = fileObject.getContent().getLastModifiedTime();
            } catch (FileSystemException e) {
                log.info("Error in getting the last modified time of the file");
                return false;
            }
            long time = System.currentTimeMillis();
            isAccepted = (time - age) / 1000 <= vfsConfig.getMinimumAge();
        }
        if (vfsConfig.getMaximumAge() != null) {
            long age = 0;
            try {
                age = fileObject.getContent().getLastModifiedTime();
            } catch (FileSystemException e) {
                log.info("Error in getting the last modified time of the file");
                return false;
            }
            long time = System.currentTimeMillis();
            isAccepted = (time - age) / 1000 >= vfsConfig.getMaximumAge();
        }
        return isAccepted;
    }
}
