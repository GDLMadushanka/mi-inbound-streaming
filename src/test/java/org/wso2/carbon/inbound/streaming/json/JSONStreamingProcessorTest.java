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

package org.wso2.carbon.inbound.streaming.json;

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

public class JSONStreamingProcessorTest {

    private static final String NESTED =
            "{ \"store\": { \"books\": [" +
            "  {\"id\":1,\"title\":\"A\",\"author\":\"Ann\"}," +
            "  {\"id\":2,\"title\":\"B\",\"author\":\"Bob\"}," +
            "  {\"id\":3,\"title\":\"C\",\"author\":\"Cara\"}" +
            "] } }";

    private static final String TOP_ARRAY =
            "[ {\"id\":1,\"name\":\"John\"}, {\"id\":2,\"name\":\"Jane\"} ]";

    private InputStream stream(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testRecordModeNestedPath() throws StreamingException {
        JSONStreamingProcessor processor =
                new JSONStreamingProcessor(8192, "$.store.books[*]", true);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(NESTED), "application/json");

        StreamRecord r1 = it.next();
        Assert.assertEquals(1, r1.getRecordNumber());
        JsonObject book1 = r1.getJSONPayload().getAsJsonObject();
        Assert.assertEquals(1, book1.get("id").getAsInt());
        Assert.assertEquals("A", book1.get("title").getAsString());

        Assert.assertTrue(it.hasNext());
        it.next(); // book 2
        StreamRecord r3 = it.next();
        JsonObject book3 = r3.getJSONPayload().getAsJsonObject();
        Assert.assertEquals("Cara", book3.get("author").getAsString());

        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testRecordModeTopLevelArrayRawContent() throws StreamingException {
        JSONStreamingProcessor processor =
                new JSONStreamingProcessor(8192, "$[*]", false);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(TOP_ARRAY), "application/json");

        StreamRecord r1 = it.next();
        // Raw mode: content is the element's compact JSON.
        Assert.assertEquals("{\"id\":1,\"name\":\"John\"}", r1.getAsString());
        StreamRecord r2 = it.next();
        Assert.assertEquals("{\"id\":2,\"name\":\"Jane\"}", r2.getAsString());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testTrailingNavigation() throws StreamingException {
        // $.store.books[*].author -> each author scalar
        JSONStreamingProcessor processor =
                new JSONStreamingProcessor(8192, "$.store.books[*].author", true);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(NESTED), "application/json");

        Assert.assertEquals("Ann", it.next().getJSONPayload().getAsString());
        Assert.assertEquals("Bob", it.next().getJSONPayload().getAsString());
        Assert.assertEquals("Cara", it.next().getJSONPayload().getAsString());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testChunkModeVariableOutput() throws StreamingException {
        JSONStreamingProcessor processor =
                new JSONStreamingProcessor(8192, "$.store.books[*]", true);
        Iterator<StreamChunk> it = processor.getChunkIterator(stream(NESTED), "application/json", 2);

        StreamChunk c1 = it.next();
        Assert.assertEquals(2, c1.getRecordCount());
        JsonArray payload1 = c1.getJSONPayload().getAsJsonArray();
        Assert.assertEquals(2, payload1.size());
        JsonObject first = payload1.get(0).getAsJsonObject();
        Assert.assertEquals("A", first.get("title").getAsString());
        Assert.assertFalse(c1.isLastChunk());

        StreamChunk c2 = it.next();
        Assert.assertEquals(1, c2.getRecordCount());
        Assert.assertTrue(c2.isLastChunk());
        Assert.assertFalse(it.hasNext());
    }


    @Test
    public void testSingleNodeNoWildcard() throws StreamingException {
        JSONStreamingProcessor processor =
                new JSONStreamingProcessor(8192, "$.store", true);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(NESTED), "application/json");

        Assert.assertTrue(it.hasNext());
        StreamRecord r = it.next();
        JsonObject store = r.getJSONPayload().getAsJsonObject();
        Assert.assertTrue(store.has("books"));
        Assert.assertFalse("Single-node selector emits exactly once", it.hasNext());
    }

    @Test
    public void testPathNotFoundYieldsNothing() throws StreamingException {
        JSONStreamingProcessor processor =
                new JSONStreamingProcessor(8192, "$.store.magazines[*]", true);
        Iterator<StreamRecord> it = processor.getRecordIterator(stream(NESTED), "application/json");
        Assert.assertFalse(it.hasNext());
    }

    @Test(expected = StreamingException.class)
    public void testRejectRecursiveDescent() throws StreamingException {
        new JSONStreamingProcessor(8192, "$..book", true)
                .getRecordIterator(stream(NESTED), "application/json");
    }

    @Test(expected = StreamingException.class)
    public void testRejectFilter() throws StreamingException {
        new JSONStreamingProcessor(8192, "$.store.books[?(@.id>1)]", true)
                .getRecordIterator(stream(NESTED), "application/json");
    }

    @Test(expected = StreamingException.class)
    public void testRejectArrayIndex() throws StreamingException {
        new JSONStreamingProcessor(8192, "$.store.books[0]", true)
                .getRecordIterator(stream(NESTED), "application/json");
    }

    @Test(expected = StreamingException.class)
    public void testRejectMultipleWildcards() throws StreamingException {
        new JSONStreamingProcessor(8192, "$.a[*].b[*]", true)
                .getRecordIterator(stream(NESTED), "application/json");
    }
}
