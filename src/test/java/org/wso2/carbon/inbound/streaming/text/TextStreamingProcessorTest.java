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

package org.wso2.carbon.inbound.streaming.text;

import com.google.gson.JsonArray;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.carbon.inbound.streaming.StreamChunk;
import org.wso2.carbon.inbound.streaming.StreamRecord;
import org.wso2.carbon.inbound.streaming.StreamingException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class TextStreamingProcessorTest {

    private static final String LOG =
            "2026-07-06 INFO started\n" +
            "2026-07-06 WARN disk low\n" +
            "2026-07-06 ERROR crash\n";

    private InputStream stream(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testRecordModeRawContent() throws StreamingException {
        TextStreamingProcessor processor = new TextStreamingProcessor(8192, false);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(LOG), "text/plain");

        StreamRecord r1 = it.next();
        Assert.assertEquals(1, r1.getRecordNumber());
        Assert.assertEquals("2026-07-06 INFO started", r1.getAsString());

        StreamRecord r2 = it.next();
        Assert.assertEquals(2, r2.getRecordNumber());
        Assert.assertEquals("2026-07-06 WARN disk low", r2.getAsString());

        StreamRecord r3 = it.next();
        Assert.assertEquals("2026-07-06 ERROR crash", r3.getAsString());

        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testRecordModeVariableOutput() throws StreamingException {
        TextStreamingProcessor processor = new TextStreamingProcessor(8192, true);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(LOG), "text/plain");

        StreamRecord r1 = it.next();
        // In variable mode the raw content is not set; the line is exposed as the payload value.
        Assert.assertNull(r1.getContent());
        Assert.assertEquals("2026-07-06 INFO started", r1.getJSONPayload().getAsString());
    }

    @Test
    public void testChunkModeRawContent() throws StreamingException {
        TextStreamingProcessor processor = new TextStreamingProcessor(8192, false);
        Iterator<StreamChunk> it = processor.getChunkIterator(stream(LOG), "text/plain", 2);

        StreamChunk c1 = it.next();
        Assert.assertEquals(2, c1.getRecordCount());
        Assert.assertEquals("2026-07-06 INFO started", c1.getRecords().get(0).getAsString());
        Assert.assertEquals("2026-07-06 WARN disk low", c1.getRecords().get(1).getAsString());
        Assert.assertFalse(c1.isLastChunk());

        StreamChunk c2 = it.next();
        Assert.assertEquals(1, c2.getRecordCount());
        Assert.assertEquals("2026-07-06 ERROR crash", c2.getRecords().get(0).getAsString());
        Assert.assertTrue(c2.isLastChunk());

        Assert.assertFalse(it.hasNext());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testChunkModeVariableOutput() throws StreamingException {
        TextStreamingProcessor processor = new TextStreamingProcessor(8192, true);
        Iterator<StreamChunk> it = processor.getChunkIterator(stream(LOG), "text/plain", 2);

        StreamChunk c1 = it.next();
        JsonArray payload = c1.getJSONPayload().getAsJsonArray();
        Assert.assertEquals(2, payload.size());
        Assert.assertEquals("2026-07-06 INFO started", payload.get(0).getAsString());
        Assert.assertEquals("2026-07-06 WARN disk low", payload.get(1).getAsString());

        StreamChunk c2 = it.next();
        JsonArray payload2 = c2.getJSONPayload().getAsJsonArray();
        Assert.assertEquals(1, payload2.size());
        Assert.assertEquals("2026-07-06 ERROR crash", payload2.get(0).getAsString());
    }

    @Test
    public void testBlankLinesPreserved() throws StreamingException {
        String withBlank = "line1\n\nline3\n";
        TextStreamingProcessor processor = new TextStreamingProcessor(8192, false);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(withBlank), "text/plain");

        Assert.assertEquals("line1", it.next().getAsString());
        Assert.assertEquals("", it.next().getAsString());   // blank line kept as a record
        Assert.assertEquals("line3", it.next().getAsString());
        Assert.assertFalse(it.hasNext());
    }


    @Test
    public void testEmptyInput() throws StreamingException {
        TextStreamingProcessor processor = new TextStreamingProcessor(8192, false);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(""), "text/plain");
        Assert.assertFalse(it.hasNext());
    }
}
