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

package org.wso2.carbon.inbound.streaming.filter;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.wso2.carbon.inbound.streaming.VFSConfig;
import org.wso2.carbon.inbound.streaming.VFSConstants;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SizeCheckFilterTest {

    private static FileSystemManager fsManager;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @BeforeClass
    public static void setUp() throws Exception {
        fsManager = VFS.getManager();
    }

    private static SizeCheckFilter filter(long intervalMs, boolean ignoreEmpty) {
        Properties p = new Properties();
        p.setProperty(VFSConstants.TRANSPORT_CHECK_SIZE_INTERVAL, String.valueOf(intervalMs));
        p.setProperty(VFSConstants.TRANSPORT_CHECK_SIZE_IGNORE_EMPTY, String.valueOf(ignoreEmpty));
        return new SizeCheckFilter(new VFSConfig(p), fsManager);
    }

    private FileObject vfs(File f) throws Exception {
        return fsManager.resolveFile(f.toURI().toString());
    }

    @Test
    public void acceptsAFileThatIsNotChanging() throws Exception {
        File f = tmp.newFile("stable.csv");
        Files.write(f.toPath(), "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8));
        assertTrue(filter(200, false).accept(vfs(f)));
    }

    @Test
    public void holdsBackAFileThatIsStillGrowing() throws Exception {
        File f = tmp.newFile("growing.csv");
        Files.write(f.toPath(), "a,b\n".getBytes(StandardCharsets.UTF_8));
        FileObject file = vfs(f);
        // Resolve the size once first, so the filter has to see past VFS's cached attributes.
        file.getContent().getSize();
        Thread writer = new Thread(() -> {
            try (OutputStream out = new FileOutputStream(f, true)) {
                for (int i = 0; i < 20; i++) {
                    out.write(("row" + i + ",x\n").getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    Thread.sleep(20);
                }
            } catch (Exception ignored) {
                // test thread
            }
        });
        writer.start();
        try {
            assertFalse(filter(150, false).accept(file));
        } finally {
            writer.join();
        }
    }

    @Test
    public void emptyFileIsHeldBackOnlyWhenIgnoreEmptyIsSet() throws Exception {
        File f = tmp.newFile("empty.csv");
        assertFalse(filter(50, true).accept(vfs(f)));
        assertTrue(filter(50, false).accept(vfs(f)));
    }

    @Test
    public void ignoreEmptyStillChecksANonEmptyFileForGrowth() throws Exception {
        // Previously CheckSizeIgnoreEmpty=true switched off the whole check.
        File f = tmp.newFile("growing2.csv");
        Files.write(f.toPath(), "a\n".getBytes(StandardCharsets.UTF_8));
        Thread writer = new Thread(() -> {
            try (OutputStream out = new FileOutputStream(f, true)) {
                for (int i = 0; i < 20; i++) {
                    out.write("more\n".getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    Thread.sleep(20);
                }
            } catch (Exception ignored) {
                // test thread
            }
        });
        writer.start();
        try {
            assertFalse(filter(150, true).accept(vfs(f)));
        } finally {
            writer.join();
        }
    }

    @Test
    public void largeFileIsCheckedWithoutReadingIt() throws Exception {
        // 64 MB: hashing it twice used to take a noticeable while; reading metadata does not.
        File f = tmp.newFile("big.csv");
        try (OutputStream out = new FileOutputStream(f)) {
            byte[] block = new byte[1 << 20];
            for (int i = 0; i < 64; i++) {
                out.write(block);
            }
        }
        long start = System.nanoTime();
        assertTrue(filter(10, false).accept(vfs(f)));
        long ms = (System.nanoTime() - start) / 1_000_000;
        assertTrue("took " + ms + " ms", ms < 500);
    }

    @Test
    public void missingFileIsNotAccepted() throws Exception {
        File f = new File(tmp.getRoot(), "gone.csv");
        assertFalse(filter(10, false).accept(vfs(f)));
    }
}
