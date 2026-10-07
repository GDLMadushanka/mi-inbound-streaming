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

public class StreamingCheckpointTest {

    private StreamingCheckpoint sample() {
        StreamingCheckpoint cp = new StreamingCheckpoint();
        cp.setSchemaVersion(1);
        cp.setInboundName("MyFileInbound");
        cp.setFileUriHash("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08");
        cp.setFileFingerprint(new StreamingCheckpoint.Fingerprint(1024L, 1700000000000L, 65536, "CRC32", "124abd"));
        cp.setStreamingMode("RECORD");
        cp.setInputFormat("csv");
        cp.setConfigHash("3f9c");
        cp.setRecordsConsumed(5012);
        cp.setProcessedRecords(5000);
        cp.setParseFailedRecords(0);
        cp.setMediationFailedRecords(12);
        cp.setUpdatedAt(1700000005000L);
        return cp;
    }

    @Test
    public void testJsonRoundTripIsCompact() {
        String json = sample().toJson();
        Assert.assertFalse("checkpoint JSON must be single-line (registry strips newlines)",
                json.contains("\n"));

        StreamingCheckpoint back = StreamingCheckpoint.fromJson(json);
        Assert.assertNotNull(back);
        Assert.assertEquals(5012, back.getRecordsConsumed());
        Assert.assertEquals(5000, back.getProcessedRecords());
        Assert.assertEquals(12, back.getMediationFailedRecords());
        Assert.assertEquals("csv", back.getInputFormat());
        Assert.assertEquals("124abd", back.getFileFingerprint().getHash());
        Assert.assertEquals(65536, back.getFileFingerprint().getHashedBytes());
    }

    @Test
    public void testFromJsonRejectsGarbage() {
        Assert.assertNull(StreamingCheckpoint.fromJson(null));
        Assert.assertNull(StreamingCheckpoint.fromJson(""));
        Assert.assertNull(StreamingCheckpoint.fromJson("this is not json"));
    }

    @Test
    public void testResumableOnlyWhenEverythingMatches() {
        StreamingCheckpoint cp = sample();
        StreamingCheckpoint.Fingerprint same =
                new StreamingCheckpoint.Fingerprint(1024L, 1700000000000L, 65536, "CRC32", "124abd");
        Assert.assertTrue(cp.isResumableFor(same, "3f9c", "RECORD", "csv"));

        // size / mtime / hash change => different file => not resumable
        Assert.assertFalse(cp.isResumableFor(
                new StreamingCheckpoint.Fingerprint(2048L, 1700000000000L, 65536, "CRC32", "124abd"),
                "3f9c", "RECORD", "csv"));
        Assert.assertFalse(cp.isResumableFor(
                new StreamingCheckpoint.Fingerprint(1024L, 1700000000000L, 65536, "CRC32", "ffff"),
                "3f9c", "RECORD", "csv"));
        // config / mode / format change => not resumable
        Assert.assertFalse(cp.isResumableFor(same, "OTHER", "RECORD", "csv"));
        Assert.assertFalse(cp.isResumableFor(same, "3f9c", "CHUNK", "csv"));
        Assert.assertFalse(cp.isResumableFor(same, "3f9c", "RECORD", "json"));
    }
}
