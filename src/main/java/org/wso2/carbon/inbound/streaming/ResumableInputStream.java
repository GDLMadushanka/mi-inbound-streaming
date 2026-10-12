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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An {@link InputStream} over a remote file that survives a dropped connection.
 * <p>
 * Streaming reads a file through one stream for as long as it takes (a 20,000-row file takes over an hour when each
 * row calls a slow service). On SFTP that stream is a single channel, and a network drop ("Pipe closed", "Connection
 * reset") makes the next {@code read} throw. Without this class the reader turned that into a non-recoverable error,
 * failed the whole file and cleared its checkpoint, although nothing in the data was wrong.
 * <p>
 * This wrapper counts the bytes it has handed on. When a read fails it closes the broken stream, waits, opens the file
 * again (over a fresh connection), skips the bytes already delivered and carries on at exactly that byte: the parser on
 * top never sees the drop, and no record is repeated or lost. The number of consecutive failures without progress is
 * bounded; when the budget is spent the original error is thrown and the existing failure handling applies.
 * <p>
 * It also refuses to end early: if the underlying stream reports end-of-file before the size the file had when it was
 * opened, that is a truncated read, not the end of the file, and is treated like a drop.
 */
public class ResumableInputStream extends InputStream {

    private static final Log log = LogFactory.getLog(ResumableInputStream.class);

    /** Opens the underlying file; {@code reopen} is true after a failure (the implementation should drop any cached connection). */
    @FunctionalInterface
    public interface Opener {
        InputStream open(boolean reopen) throws IOException;
    }

    private final Opener opener;
    private final String what;
    private final long expectedSize;
    private final int maxRetries;
    private final long retryDelayMs;

    private InputStream current;
    private long position;
    private int failures;
    private int reopens;

    /**
     * @param opener       opens the file's stream
     * @param what         a name for log lines: the file's base name, never its URI (the URI can carry the password)
     * @param expectedSize the file size at the start, or a negative number if unknown (the early-end check is then off)
     * @param maxRetries   consecutive failures without progress tolerated; 0 turns the behaviour off
     * @param retryDelayMs pause before a retry, multiplied by the failure count (a longer pause for each repeat)
     */
    public ResumableInputStream(Opener opener, String what, long expectedSize, int maxRetries, long retryDelayMs)
            throws IOException {
        this.opener = opener;
        this.what = what;
        this.expectedSize = expectedSize;
        this.maxRetries = Math.max(0, maxRetries);
        this.retryDelayMs = Math.max(0, retryDelayMs);
        // The very first open follows the same retry budget (a connection refused at that moment is also transient).
        while (true) {
            try {
                this.current = opener.open(false);
                return;
            } catch (IOException e) {
                afterFailure(e, "open");
            }
        }
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : (one[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        while (true) {
            try {
                if (current == null) {
                    reopen();
                }
                int n = current.read(b, off, len);
                if (n > 0) {
                    position += n;
                    failures = 0;
                    return n;
                }
                if (n < 0 && expectedSize >= 0 && position < expectedSize) {
                    throw new EOFException("end of stream after " + position + " of " + expectedSize + " bytes");
                }
                return n;
            } catch (IOException e) {
                afterFailure(e, "read");
                closeQuietly();
            }
        }
    }

    @Override
    public int available() throws IOException {
        try {
            return current == null ? 0 : current.available();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public void close() throws IOException {
        InputStream in = current;
        current = null;
        if (in != null) {
            in.close();
        }
    }

    /** How many times the file was opened again after a failure (0 when nothing went wrong). */
    public int getReopenCount() {
        return reopens;
    }

    /** Bytes handed on so far. */
    public long getPosition() {
        return position;
    }

    private void reopen() throws IOException {
        InputStream in = opener.open(true);
        try {
            long remaining = position;
            while (remaining > 0) {
                long skipped = in.skip(remaining);
                if (skipped <= 0) {
                    if (in.read() < 0) {
                        throw new EOFException("the file is shorter than the " + position + " bytes already read");
                    }
                    skipped = 1;
                }
                remaining -= skipped;
            }
        } catch (IOException | RuntimeException e) {
            try {
                in.close();
            } catch (IOException ignore) {
                // the original failure is the one that matters
            }
            throw e;
        }
        current = in;
        reopens++;
        log.info("Reading of " + what + " resumed at byte " + position + " after a connection failure (reopen "
                + reopens + ").");
    }

    /** Spend one unit of the failure budget, pause, or give up by rethrowing the failure. */
    private void afterFailure(IOException cause, String during) throws IOException {
        if (++failures > maxRetries) {
            throw cause;
        }
        log.warn("Reading of " + what + " failed during " + during + " at byte " + position + " (failure " + failures
                + " of " + maxRetries + " tolerated): " + cause + ". Reopening the file and resuming.");
        long pause = retryDelayMs * failures;
        if (pause > 0) {
            try {
                Thread.sleep(pause);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                InterruptedIOException stop = new InterruptedIOException("interrupted while waiting to resume " + what);
                stop.initCause(cause);
                throw stop;
            }
        }
    }

    private void closeQuietly() {
        InputStream in = current;
        current = null;
        if (in != null) {
            try {
                in.close();
            } catch (IOException ignore) {
                // already broken
            }
        }
    }
}
