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

import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

/**
 * The SFTP socket timeouts have to reach the FileSystemOptions. jsch waits forever without them,
 * so a source that accepts a connection and then stops responding blocks the polling thread
 * indefinitely - including on the metadata calls made before a file is read, such as the
 * exists() in Utils.isFailRecord.
 */
public class SftpTimeoutOptionsTest {

    private static FileSystemManager fsManager;
    private static final SftpFileSystemConfigBuilder SFTP = SftpFileSystemConfigBuilder.getInstance();
    private static final String URI = "sftp://user:pass@host/incoming";

    @BeforeClass
    public static void setUp() throws Exception {
        fsManager = VFS.getManager();
    }

    /** The real path: inbound parameters -> option map -> FileSystemOptions. */
    private FileSystemOptions optionsFrom(Properties params) throws Exception {
        Map<String, String> parsed = Utils.parseSchemeFileOptions(URI, params);
        return Utils.attachFileSystemOptions(parsed, fsManager);
    }

    private FileSystemOptions optionsFromUri(String uri) throws Exception {
        return Utils.attachFileSystemOptions(
            Utils.parseSchemeFileOptions(uri, new Properties()), fsManager);
    }

    private static Properties params(String key, String value) {
        Properties p = new Properties();
        p.setProperty(key, value);
        return p;
    }

    @Test
    public void testReadTimeoutIsUnsetByDefault() throws Exception {
        // Deliberately not defaulted: it also bounds reads taken while a file is streaming, and
        // those are paced by how fast the sequence mediates.
        Assert.assertNull(SFTP.getTimeout(optionsFrom(new Properties())));
    }

    @Test
    public void testReadTimeoutParameterIsApplied() throws Exception {
        FileSystemOptions opts = optionsFrom(params("transport.vfs.SFTPTimeout", "45000"));
        Assert.assertEquals(Integer.valueOf(45000), SFTP.getTimeout(opts));
    }

    @Test
    public void testConnectTimeoutIsAppliedEvenWhenNothingIsConfigured() throws Exception {
        // A TCP connect has no legitimate unbounded case, so this one defaults rather than
        // inheriting jsch's "wait forever".
        Assert.assertEquals(Integer.valueOf(VFSConstants.DEFAULT_SFTP_CONNECT_TIMEOUT),
            SFTP.getConnectTimeout(optionsFrom(new Properties())));
    }

    @Test
    public void testConnectTimeoutParameterOverridesTheDefault() throws Exception {
        Assert.assertEquals(Integer.valueOf(5000),
            SFTP.getConnectTimeout(optionsFrom(params("transport.vfs.SFTPConnectTimeout", "5000"))));
    }

    @Test
    public void testBothTimeoutsTogether() throws Exception {
        Properties p = params("transport.vfs.SFTPTimeout", "20000");
        p.setProperty("transport.vfs.SFTPConnectTimeout", "10000");
        FileSystemOptions opts = optionsFrom(p);
        Assert.assertEquals(Integer.valueOf(20000), SFTP.getTimeout(opts));
        Assert.assertEquals(Integer.valueOf(10000), SFTP.getConnectTimeout(opts));
    }

    @Test
    public void testApplyingOptionsNeverNeedsJschOnTheClasspath() throws Exception {
        // Guard against reintroducing a Class.getMethod lookup on SftpFileSystemConfigBuilder:
        // that resolves every public signature, one of which references com.jcraft.jsch.UserInfo,
        // and would fail the whole Utils class initialiser wherever jsch is not loadable. This
        // test JVM has no jsch, so simply reaching the assertion proves the point.
        Assert.assertNotNull(optionsFrom(params("transport.vfs.SFTPConnectTimeout", "7000")));
    }

    @Test
    public void testTimeoutsCanAlsoComeFromTheFileUriQuery() throws Exception {
        FileSystemOptions opts = optionsFromUri(URI + "?Timeout=15000&ConnectTimeout=3000");
        Assert.assertEquals(Integer.valueOf(15000), SFTP.getTimeout(opts));
        Assert.assertEquals(Integer.valueOf(3000), SFTP.getConnectTimeout(opts));
    }

    @Test
    public void testANonNumericValueIsIgnoredRatherThanFailingTheInbound() throws Exception {
        // One typo in an optional tuning parameter must not stop the endpoint deploying.
        FileSystemOptions opts = optionsFrom(params("transport.vfs.SFTPTimeout", "thirty seconds"));
        Assert.assertNull(SFTP.getTimeout(opts));
        Assert.assertEquals(Integer.valueOf(VFSConstants.DEFAULT_SFTP_CONNECT_TIMEOUT),
            SFTP.getConnectTimeout(opts));
    }

    @Test
    public void testANonPositiveTimeoutIsIgnored() throws Exception {
        // 0 is jsch's "infinite"; accepting it would reintroduce the hang the parameter exists
        // to prevent.
        Assert.assertNull(SFTP.getTimeout(optionsFrom(params("transport.vfs.SFTPTimeout", "0"))));
        Assert.assertNull(SFTP.getTimeout(optionsFrom(params("transport.vfs.SFTPTimeout", "-1"))));

        Assert.assertEquals("a rejected value must fall back to the default, not to infinite",
            Integer.valueOf(VFSConstants.DEFAULT_SFTP_CONNECT_TIMEOUT),
            SFTP.getConnectTimeout(optionsFrom(params("transport.vfs.SFTPConnectTimeout", "0"))));
    }

    @Test
    public void testTheExistingStringSftpOptionsStillWork() throws Exception {
        // The timeouts are skipped in the delegating-builder loop; the others must still go
        // through. Unlike the typed builder the delegating one resolves the scheme through the
        // manager, so it needs the sftp provider actually registered - which it is on the server
        // but not in a bare test JVM without jsch.
        Assume.assumeTrue("no sftp provider on the test classpath",
            fsManager.hasProvider(VFSConstants.SCHEME_SFTP));
        FileSystemOptions opts = optionsFrom(params("transport.vfs.SFTPUserDirIsRoot", "false"));
        Assert.assertEquals(Boolean.FALSE, SFTP.getUserDirIsRoot(opts));
    }
}
