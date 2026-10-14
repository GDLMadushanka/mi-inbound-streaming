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
 * The checkpoint kept until the file has been moved: what the jar relies on when a node dies after the last record
 * but before the move. The handler itself needs a Synapse environment, so the end-to-end behaviour (the checkpoint
 * survives the completion callback, a restart reads nothing new and completes the file, the move clears it) is
 * covered by the cluster tests; these cover the checkpoint contract it depends on.
 */
public class CheckpointKeptUntilMovedTest {

    private static final long SIZE = 1024L;
    private static final long MTIME = 1700000000000L;

    private StreamingCheckpoint atLastRecord(long records) {
        StreamingCheckpoint cp = new StreamingCheckpoint();
        cp.setSchemaVersion(1);
        cp.setInboundName("MyFileInbound");
        cp.setFileFingerprint(new StreamingCheckpoint.Fingerprint(SIZE, MTIME, 65536, "CRC32", "124abd"));
        cp.setStreamingMode("RECORD");
        cp.setInputFormat("csv");
        cp.setConfigHash("3f9c");
        cp.setRecordsConsumed(records);
        cp.setProcessedRecords(records);
        return cp;
    }

    /** A checkpoint saved at the last record is resumable for the same file: a restart must not be told to start over. */
    @Test
    public void finalPositionCheckpointIsResumableAndKeepsItsCount() {
        StreamingCheckpoint stored = StreamingCheckpoint.fromJson(atLastRecord(10).toJson());
        Assert.assertNotNull(stored);
        Assert.assertEquals("the saved position is the last record", 10, stored.getRecordsConsumed());
        Assert.assertNull("nothing may make a finished file's checkpoint non-resumable",
                stored.whyNotResumableFor(new StreamingCheckpoint.Fingerprint(SIZE, MTIME, 65536, "CRC32", "124abd"),
                        "3f9c", "RECORD", "csv"));
    }

    /**
     * A checkpoint left behind after the move (a crash between the move and the clear) must not be applied to a
     * file with different content that arrives later under the same name. A file's identity is its byte count plus a
     * hash of its leading bytes; the modification time is deliberately ignored (see Fingerprint#matches). The
     * flip side, by design: a file with the same size and the same leading bytes is treated as the same file.
     */
    @Test
    public void staleCheckpointIsNotAppliedToAnotherFile() {
        StreamingCheckpoint stored = atLastRecord(10);
        StreamingCheckpoint.Fingerprint[] other = {
                new StreamingCheckpoint.Fingerprint(SIZE + 1, MTIME, 65536, "CRC32", "124abd"),   // different size
                new StreamingCheckpoint.Fingerprint(SIZE, MTIME, 65536, "CRC32", "ffff")           // different content
        };
        for (StreamingCheckpoint.Fingerprint f : other) {
            Assert.assertNotNull("a different file must not resume from the stale checkpoint",
                    stored.whyNotResumableFor(f, "3f9c", "RECORD", "csv"));
        }
        // same bytes, new timestamp: the same file (a file moved away and copied back keeps its checkpoint)
        Assert.assertNull(stored.whyNotResumableFor(
                new StreamingCheckpoint.Fingerprint(SIZE, MTIME + 1000, 65536, "CRC32", "124abd"), "3f9c", "RECORD", "csv"));
    }
}
