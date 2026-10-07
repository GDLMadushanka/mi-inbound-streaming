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

package org.wso2.carbon.inbound.streaming.jsonl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.carbon.inbound.streaming.StreamChunk;
import org.wso2.carbon.inbound.streaming.StreamRecord;
import org.wso2.carbon.inbound.streaming.StreamingException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class JSONLStreamingProcessorTest {

    private static final String VALID =
            "{\"id\":1,\"name\":\"John\"}\n" +
            "{\"id\":2,\"name\":\"Jane\"}\n" +
            "{\"id\":3,\"name\":\"Bob\"}\n";

    // Second line is malformed; JSONL should keep going, marking only that line invalid.
    private static final String WITH_BAD_LINE =
            "{\"id\":1,\"name\":\"John\"}\n" +
            "{\"id\":2,\"name\":  \n" +
            "{\"id\":3,\"name\":\"Bob\"}\n";

    private InputStream stream(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testRecordModeRawContent() throws StreamingException {
        JSONLStreamingProcessor processor = new JSONLStreamingProcessor(8192, false);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(VALID), "application/json");

        StreamRecord r1 = it.next();
        Assert.assertEquals(1, r1.getRecordNumber());
        Assert.assertTrue(r1.isValid());
        Assert.assertEquals("{\"id\":1,\"name\":\"John\"}", r1.getAsString());

        Assert.assertEquals("{\"id\":2,\"name\":\"Jane\"}", it.next().getAsString());
        Assert.assertEquals("{\"id\":3,\"name\":\"Bob\"}", it.next().getAsString());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testRecordModeVariableOutput() throws StreamingException {
        JSONLStreamingProcessor processor = new JSONLStreamingProcessor(8192, true);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(VALID), "application/json");

        StreamRecord r1 = it.next();
        // In variable mode the raw content is not set; the line is exposed as the payload value.
        Assert.assertNull(r1.getContent());
        JsonObject payload = r1.getJSONPayload().getAsJsonObject();
        Assert.assertEquals(1, payload.get("id").getAsInt());
        Assert.assertEquals("John", payload.get("name").getAsString());
    }

    @Test
    public void testBlankLinesSkipped() throws StreamingException {
        String withBlanks = "{\"id\":1}\n\n   \n{\"id\":2}\n";
        JSONLStreamingProcessor processor = new JSONLStreamingProcessor(8192, false);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(withBlanks), "application/json");

        StreamRecord r1 = it.next();
        Assert.assertEquals("{\"id\":1}", r1.getAsString());
        StreamRecord r2 = it.next();
        Assert.assertEquals("{\"id\":2}", r2.getAsString());
        Assert.assertEquals(2, r2.getRecordNumber());   // blanks do not consume record numbers
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testMalformedLineIsRecoverableInvalidRecord() throws StreamingException {
        JSONLStreamingProcessor processor = new JSONLStreamingProcessor(8192, false);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(WITH_BAD_LINE), "application/json");

        StreamRecord r1 = it.next();
        Assert.assertTrue(r1.isValid());

        // The malformed line is surfaced as an invalid record, not a thrown exception, and its raw
        // bytes are preserved so the caller can siphon them.
        StreamRecord bad = it.next();
        Assert.assertEquals(2, bad.getRecordNumber());
        Assert.assertFalse(bad.isValid());
        Assert.assertNotNull(bad.getParseError());
        Assert.assertEquals("{\"id\":2,\"name\":", bad.getAsString().trim());

        // Iteration continues past the bad line.
        StreamRecord r3 = it.next();
        Assert.assertTrue(r3.isValid());
        Assert.assertEquals("{\"id\":3,\"name\":\"Bob\"}", r3.getAsString());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testChunkModeVariableOutputExcludesInvalidFromPayload() throws StreamingException {
        JSONLStreamingProcessor processor = new JSONLStreamingProcessor(8192, true);
        Iterator<StreamChunk> it = processor.getChunkIterator(stream(WITH_BAD_LINE), "application/json", 3);

        StreamChunk chunk = it.next();
        // All three lines are in the chunk (so invalid ones can be siphoned)...
        Assert.assertEquals(3, chunk.getRecordCount());
        Assert.assertEquals(3, chunk.getRecords().size());
        Assert.assertFalse(chunk.getRecords().get(1).isValid());
        // ...but only the two valid records appear in the variable payload.
        JsonArray payload = chunk.getJSONPayload().getAsJsonArray();
        Assert.assertEquals(2, payload.size());
        Assert.assertEquals(1, payload.get(0).getAsJsonObject().get("id").getAsInt());
        Assert.assertEquals(3, payload.get(1).getAsJsonObject().get("id").getAsInt());
        Assert.assertTrue(chunk.isLastChunk());
    }

    @Test
    public void testChunkModeRawContent() throws StreamingException {
        JSONLStreamingProcessor processor = new JSONLStreamingProcessor(8192, false);
        Iterator<StreamChunk> it = processor.getChunkIterator(stream(VALID), "application/json", 2);

        StreamChunk c1 = it.next();
        Assert.assertEquals(2, c1.getRecordCount());
        Assert.assertFalse(c1.isLastChunk());
        StreamChunk c2 = it.next();
        Assert.assertEquals(1, c2.getRecordCount());
        Assert.assertTrue(c2.isLastChunk());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testBuildChunkBodyIsJsonArray() throws StreamingException {
        JSONLStreamingProcessor processor = new JSONLStreamingProcessor(8192, false);
        Iterator<StreamChunk> it = processor.getChunkIterator(stream(WITH_BAD_LINE), "application/json", 3);

        StreamChunk chunk = it.next();
        // Raw-mode chunk body is a JSON array of the valid lines; the malformed line is excluded.
        String body = new String(processor.buildChunkBody(chunk), StandardCharsets.UTF_8);
        Assert.assertEquals("[{\"id\":1,\"name\":\"John\"},{\"id\":3,\"name\":\"Bob\"}]", body);
    }


    @Test
    public void testEmptyInput() throws StreamingException {
        JSONLStreamingProcessor processor = new JSONLStreamingProcessor(8192, false);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(""), "application/json");
        Assert.assertFalse(it.hasNext());
    }
}
