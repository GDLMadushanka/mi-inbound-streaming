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
 * The serialized checkpoint must not carry the source file's URI in any recoverable form: it is
 * machine state written to a shared registry mount, and a remote URI carries credentials.
 */
public class CheckpointUriHashTest {

    private static final String SECRET_URI = "sftp://alice:s3cr3t@sftp.example.com/incoming/a.csv";
    // SHA-256 of SECRET_URI, as the manager computes it.
    private static final String EXPECTED_HASH = sha256(SECRET_URI);

    private static String sha256(String value) {
        try {
            java.security.MessageDigest d = java.security.MessageDigest.getInstance("SHA-256");
            byte[] h = d.digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(h.length * 2);
            for (byte b : h) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private StreamingCheckpoint checkpointWithHash() {
        StreamingCheckpoint cp = new StreamingCheckpoint();
        cp.setSchemaVersion(StreamingConstants.CHECKPOINT_SCHEMA_VERSION);
        cp.setInboundName("ordersInbound");
        cp.setFileUriHash(EXPECTED_HASH);
        cp.setStreamingMode("CHUNK");
        cp.setInputFormat("csv");
        cp.setConfigHash("3f9c");
        cp.setRecordsConsumed(1000);
        cp.setChunksConsumed(5);
        return cp;
    }

    @Test
    public void testSerializedCheckpointLeaksNoPartOfTheUri() {
        String json = checkpointWithHash().toJson();
        Assert.assertFalse("password", json.contains("s3cr3t"));
        Assert.assertFalse("user", json.contains("alice"));
        Assert.assertFalse("host", json.contains("sftp.example.com"));
        Assert.assertFalse("path", json.contains("incoming"));
        Assert.assertFalse("scheme", json.contains("sftp://"));
    }

    @Test
    public void testHashIsStoredAndSurvivesARoundTrip() {
        StreamingCheckpoint back = StreamingCheckpoint.fromJson(checkpointWithHash().toJson());
        Assert.assertNotNull(back);
        Assert.assertEquals(EXPECTED_HASH, back.getFileUriHash());
        Assert.assertEquals(64, back.getFileUriHash().length());
    }

    @Test
    public void testDifferentUrisHashDifferently() {
        Assert.assertNotEquals(sha256("file:///data/in/a.csv"), sha256("file:///data/in/b.csv"));
    }

    @Test
    public void testTheSameUriAlwaysHashesTheSame() {
        // The resume has to find the same checkpoint for the same file on a later run.
        Assert.assertEquals(sha256(SECRET_URI), sha256(SECRET_URI));
    }

    @Test
    public void testACheckpointWrittenBeforeThisFieldStillDeserialises() {
        // Old checkpoints carried "fileUri"; Gson ignores the unknown key and the resume decision
        // never used it, so such a checkpoint must still load and still be resumable.
        String legacy = "{\"schemaVersion\":1,\"inboundName\":\"ordersInbound\","
            + "\"fileUri\":\"file:///data/in/a.csv\",\"streamingMode\":\"CHUNK\","
            + "\"inputFormat\":\"csv\",\"configHash\":\"3f9c\",\"recordsConsumed\":1000}";
        StreamingCheckpoint cp = StreamingCheckpoint.fromJson(legacy);
        Assert.assertNotNull(cp);
        Assert.assertEquals(1000, cp.getRecordsConsumed());
        Assert.assertNull("the old key must not be mapped onto the new field", cp.getFileUriHash());
    }
}
