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

import org.wso2.carbon.inbound.streaming.StreamingCheckpoint.Fingerprint;

/**
 * A refused resume silently costs a full re-read of the source file, and before these reasons
 * existed it was indistinguishable in the log from "no checkpoint". Each rejection must name itself.
 */
public class CheckpointResumeReasonTest {

    private static final String MODE = "CHUNK";
    private static final String FORMAT = "csv";
    private static final String CONFIG_HASH = "da5db5";

    private static Fingerprint fp(long size, long lastModified, String hash) {
        return new Fingerprint(size, lastModified, 65536, "CRC32", hash);
    }

    private static final Fingerprint CURRENT = fp(1073749726L, 1791263722988L, "5aa6208e");

    private StreamingCheckpoint checkpoint(Fingerprint stored) {
        StreamingCheckpoint cp = new StreamingCheckpoint();
        cp.setSchemaVersion(StreamingConstants.CHECKPOINT_SCHEMA_VERSION);
        cp.setInboundName("bigOrdersInbound");
        cp.setFileFingerprint(stored);
        cp.setStreamingMode(MODE);
        cp.setInputFormat(FORMAT);
        cp.setConfigHash(CONFIG_HASH);
        cp.setRecordsConsumed(1480000);
        return cp;
    }

    private String why(StreamingCheckpoint cp) {
        return cp.whyNotResumableFor(CURRENT, CONFIG_HASH, MODE, FORMAT);
    }

    @Test
    public void testAMatchingCheckpointGivesNoReasonAndIsResumable() {
        StreamingCheckpoint cp = checkpoint(CURRENT);
        Assert.assertNull(why(cp));
        Assert.assertTrue(cp.isResumableFor(CURRENT, CONFIG_HASH, MODE, FORMAT));
    }

    @Test
    public void testSubSecondMtimePrecisionBetweenNodesStillResumes() {
        // The defect this guards: one node reported the local file's mtime as ...539594 and the
        // other as ...539000, the same instant truncated to a whole second. Comparing the
        // timestamp made the second node discard a checkpoint the first had just written and
        // re-read the file from record 1.
        Fingerprint asNode1SawIt = fp(1073749726L, 1791263539594L, "5aa6208e");
        Fingerprint asNode2SawIt = fp(1073749726L, 1791263539000L, "5aa6208e");
        StreamingCheckpoint cp = checkpoint(asNode1SawIt);
        Assert.assertNull("same bytes, so the other node must resume",
            cp.whyNotResumableFor(asNode2SawIt, CONFIG_HASH, MODE, FORMAT));
        Assert.assertTrue(cp.isResumableFor(asNode2SawIt, CONFIG_HASH, MODE, FORMAT));
    }

    @Test
    public void testRecopyingTheFileKeepsTheCheckpointUsable() {
        // Moving the file out and copying it back gives identical bytes and a new timestamp.
        StreamingCheckpoint cp = checkpoint(fp(1073749726L, 1791263680000L, "5aa6208e"));
        Assert.assertNull(why(cp));
    }

    @Test
    public void testAGenuinelyDifferentFileReportsSizeAndContent() {
        StreamingCheckpoint cp = checkpoint(fp(42L, 1791263722988L, "deadbeef"));
        String reason = why(cp);
        Assert.assertNotNull(reason);
        Assert.assertTrue(reason, reason.contains("the file changed"));
        Assert.assertTrue(reason, reason.contains("size 42->1073749726"));
        Assert.assertTrue(reason, reason.contains("CRC32 deadbeef->5aa6208e"));
    }

    @Test
    public void testATruncatedFileOfTheSameNameIsNotResumed() {
        // Content identity still has to catch a real replacement.
        StreamingCheckpoint cp = checkpoint(fp(500L, 1791263722988L, "5aa6208e"));
        Assert.assertTrue(why(cp).contains("size 500->1073749726"));
    }

    @Test
    public void testADifferentHashBudgetIsNotComparedAsEqual() {
        Fingerprint other = new Fingerprint(1073749726L, 1791263722988L, 4096, "CRC32", "5aa6208e");
        StreamingCheckpoint cp = checkpoint(other);
        Assert.assertTrue(why(cp).contains("hashedBytes 4096->65536"));
    }

    @Test
    public void testATimestampDriftIsShownButLabelledNotCompared() {
        StreamingCheckpoint cp = checkpoint(fp(42L, 1791263680000L, "deadbeef"));
        String reason = why(cp);
        Assert.assertTrue(reason, reason.contains("not compared"));
    }

    @Test
    public void testAChangedConfigurationNamesBothHashes() {
        StreamingCheckpoint cp = checkpoint(CURRENT);
        cp.setConfigHash("0000");
        String reason = why(cp);
        Assert.assertTrue(reason, reason.contains("streaming configuration changed"));
        Assert.assertTrue(reason, reason.contains("checkpoint configHash=0000"));
        Assert.assertTrue(reason, reason.contains("current=" + CONFIG_HASH));
    }

    @Test
    public void testAChangedModeAndFormatAreReportedSeparately() {
        StreamingCheckpoint byMode = checkpoint(CURRENT);
        byMode.setStreamingMode("RECORD");
        Assert.assertTrue(why(byMode).contains("streaming mode changed"));

        StreamingCheckpoint byFormat = checkpoint(CURRENT);
        byFormat.setInputFormat("json");
        Assert.assertTrue(why(byFormat).contains("input format changed"));
    }

    @Test
    public void testModeAndFormatComparisonsIgnoreCase() {
        StreamingCheckpoint cp = checkpoint(CURRENT);
        cp.setStreamingMode("chunk");
        cp.setInputFormat("CSV");
        Assert.assertNull(why(cp));
    }

    @Test
    public void testACheckpointWithNoFingerprintSaysSo() {
        Assert.assertEquals("the checkpoint carries no file fingerprint", why(checkpoint(null)));
    }

    @Test
    public void testAnUncomputableCurrentFingerprintSaysSo() {
        StreamingCheckpoint cp = checkpoint(CURRENT);
        Assert.assertEquals("the current file's fingerprint could not be computed",
            cp.whyNotResumableFor(null, CONFIG_HASH, MODE, FORMAT));
    }
}
