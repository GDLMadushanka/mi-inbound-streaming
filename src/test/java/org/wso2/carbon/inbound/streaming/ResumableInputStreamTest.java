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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * A dropped connection must neither fail the file nor repeat or lose a byte: the stream reopens the file, skips what was
 * already handed on and carries on. When the budget is spent the original failure is thrown, as before.
 */
public class ResumableInputStreamTest {

    private static byte[] data(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = (byte) ('a' + (i * 7 + i / 13) % 26);
        }
        return b;
    }

    /** A stream over {@code all} that throws after {@code failAfter} bytes (or ends cleanly there when {@code silentEof}). */
    private static InputStream broken(final byte[] all, final int failAfter, final boolean silentEof) {
        return new InputStream() {
            private int pos;

            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                int n = read(one, 0, 1);
                return n < 0 ? -1 : one[0] & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (pos >= failAfter) {
                    if (silentEof) {
                        return -1;
                    }
                    throw new IOException("Pipe closed");
                }
                int n = Math.min(Math.min(len, 5), Math.min(all.length, failAfter) - pos);
                if (n <= 0) {
                    return -1;
                }
                System.arraycopy(all, pos, b, off, n);
                pos += n;
                return n;
            }
        };
    }

    /** Opener whose first stream breaks after the given number of bytes; every later stream is intact. */
    private static ResumableInputStream.Opener flaky(final byte[] all, final int[] breakAt, final List<Boolean> reopenFlags) {
        final int[] calls = {0};
        return reopen -> {
            reopenFlags.add(reopen);
            int c = calls[0]++;
            if (c < breakAt.length) {
                return broken(all, breakAt[c], false);
            }
            return new ByteArrayInputStream(all);
        };
    }

    private static byte[] drain(InputStream in, int chunk) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[chunk];
        int n;
        while ((n = in.read(buf, 0, buf.length)) >= 0) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    @Test
    public void nothingGoesWrongNothingChanges() throws IOException {
        byte[] all = data(1000);
        ResumableInputStream in = new ResumableInputStream(o -> new ByteArrayInputStream(all), "f", all.length, 3, 0);
        Assert.assertArrayEquals(all, drain(in, 64));
        Assert.assertEquals(0, in.getReopenCount());
    }

    @Test
    public void aDropMidFileResumesAtTheSameByte() throws IOException {
        byte[] all = data(1000);
        List<Boolean> flags = new ArrayList<>();
        ResumableInputStream in = new ResumableInputStream(flaky(all, new int[] {333}, flags), "f", all.length, 3, 0);
        Assert.assertArrayEquals("no byte repeated or lost", all, drain(in, 64));
        Assert.assertEquals(1, in.getReopenCount());
        Assert.assertEquals("the first open is not a reopen, the second is", java.util.Arrays.asList(false, true), flags);
    }

    @Test
    public void severalDropsEachWithinTheBudget() throws IOException {
        byte[] all = data(1000);
        ResumableInputStream in = new ResumableInputStream(flaky(all, new int[] {100, 450, 700}, new ArrayList<>()), "f", all.length, 3, 0);
        // after a reopen the stream is intact except while calls < breakAt.length; each broken stream fails again at its offset
        // relative to its own start, so progress is made between failures and the budget resets
        Assert.assertArrayEquals(all, drain(in, 7));
        Assert.assertTrue(in.getReopenCount() >= 3);
    }

    @Test
    public void aFailureRightAtTheStartOfTheReopenCountsAgainstTheBudget() throws IOException {
        final byte[] all = data(500);
        final int[] calls = {0};
        ResumableInputStream in = new ResumableInputStream(reopen -> {
            int c = calls[0]++;
            if (c == 0) {
                return broken(all, 200, false);
            }
            if (c == 1) {
                throw new IOException("connection refused");   // the reopen itself fails once
            }
            return new ByteArrayInputStream(all);
        }, "f", all.length, 3, 0);
        Assert.assertArrayEquals(all, drain(in, 50));
    }

    @Test
    public void whenTheBudgetIsSpentTheOriginalFailureIsThrown() throws IOException {
        final byte[] all = data(500);
        ResumableInputStream in = new ResumableInputStream(reopen -> broken(all, 100, false), "f", all.length, 2, 0);
        try {
            drain(in, 50);
            Assert.fail("expected the failure after the budget");
        } catch (IOException e) {
            Assert.assertEquals("Pipe closed", e.getMessage());
        }
    }

    @Test
    public void retriesZeroBehavesAsBefore() throws IOException {
        final byte[] all = data(500);
        ResumableInputStream in = new ResumableInputStream(reopen -> broken(all, 100, false), "f", all.length, 0, 0);
        try {
            drain(in, 50);
            Assert.fail("expected the first failure to surface");
        } catch (IOException e) {
            Assert.assertEquals("Pipe closed", e.getMessage());
            Assert.assertEquals(0, in.getReopenCount());
        }
    }

    @Test
    public void aStreamThatEndsEarlyIsATruncatedReadNotTheEndOfTheFile() throws IOException {
        final byte[] all = data(500);
        final int[] calls = {0};
        ResumableInputStream in = new ResumableInputStream(reopen -> calls[0]++ == 0 ? broken(all, 300, true) : new ByteArrayInputStream(all),
                "f", all.length, 3, 0);
        Assert.assertArrayEquals("the silent early end is detected and the rest is read", all, drain(in, 64));
        Assert.assertEquals(1, in.getReopenCount());
    }

    @Test
    public void unknownSizeTurnsTheEarlyEndCheckOff() throws IOException {
        final byte[] all = data(500);
        ResumableInputStream in = new ResumableInputStream(reopen -> broken(all, 300, true), "f", -1, 3, 0);
        Assert.assertEquals("without a known size the early end is accepted", 300, drain(in, 64).length);
    }

    @Test
    public void aFileThatShrankWhileWeWereAwayIsNotGuessedAt() throws IOException {
        final byte[] all = data(500);
        final int[] calls = {0};
        ResumableInputStream in = new ResumableInputStream(reopen -> calls[0]++ == 0 ? broken(all, 300, false) : new ByteArrayInputStream(data(100)),
                "f", all.length, 1, 0);
        try {
            drain(in, 64);
            Assert.fail("a file shorter than what was already read must fail, not continue");
        } catch (IOException e) {
            Assert.assertTrue(String.valueOf(e), e.getMessage().contains("shorter") || e.getMessage().contains("Pipe closed"));
        }
    }
}
