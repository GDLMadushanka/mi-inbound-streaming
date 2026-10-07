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

import org.junit.Assert;
import org.junit.Test;

/**
 * Password masking for file URIs. The streaming checkpoint persists a file URI into the registry,
 * which on a clustered deployment is a shared mount, so a remote URI must never reach it with its
 * credentials intact.
 */
public class UtilsMaskUrlPasswordTest {

    @Test
    public void testSftpPasswordIsMasked() {
        String masked = Utils.maskURLPassword("sftp://alice:s3cr3t@sftp.example.com/incoming/a.csv");
        Assert.assertFalse("the password must not survive", masked.contains("s3cr3t"));
        Assert.assertTrue(masked.contains(":***@"));
        Assert.assertTrue("the rest of the URI is still readable",
            masked.contains("sftp.example.com/incoming/a.csv"));
    }

    @Test
    public void testSmbPasswordIsMasked() {
        String masked = Utils.maskURLPassword("smb2://svc:P%40ssw0rd@host/share/in/b.csv");
        Assert.assertFalse(masked.contains("P%40ssw0rd"));
        Assert.assertTrue(masked.contains(":***@"));
    }

    @Test
    public void testLocalFileUriIsUnchanged() {
        String uri = "file:///data/in/orders.csv";
        Assert.assertEquals("nothing to mask, so it must round-trip exactly",
            uri, Utils.maskURLPassword(uri));
    }

    @Test
    public void testUriWithUserButNoPasswordIsUnchanged() {
        String uri = "sftp://alice@sftp.example.com/incoming/a.csv";
        Assert.assertEquals(uri, Utils.maskURLPassword(uri));
    }
}
