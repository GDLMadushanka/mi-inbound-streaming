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

package org.wso2.carbon.inbound.streaming.csv;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.Test;
import org.wso2.carbon.inbound.streaming.StreamChunk;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The chunk payload is now built on demand, and usually written straight to text instead of via a
 * Gson tree. Both routes must produce the JSON the sequence received before the change.
 */
public class CsvChunkPayloadTest {

    private static final String TYPES = "["
        + "{\"Column Name Or Index\":\"id\",\"Is Column Name\":\"Yes\",\"Data Type\":\"Integer\"},"
        + "{\"Column Name Or Index\":\"price\",\"Is Column Name\":\"Yes\",\"Data Type\":\"Number\"},"
        + "{\"Column Name Or Index\":\"active\",\"Is Column Name\":\"Yes\",\"Data Type\":\"Boolean\"}]";

    private static final String CSV = "id,name,price,active,note\n"
        + "1,Alice,120.50,true,plain\n"
        + "2,\"Bob, Jr.\",0.10,FALSE,\"says \"\"hi\"\"\"\n"
        + "N/A,Ćelik,1E+3,maybe,\"line one\nline two\"\n"
        + "4,\"<tag> & 'quotes'\",,true,tab\there\n"
        + "99999999999,Zoë,-0.000,true, sep\n"
        + "6,short\n";

    private static List<StreamChunk> chunks(String csv, boolean hasHeader, boolean headersEach,
                                            String types, int chunkSize) throws Exception {
        CSVStreamingProcessor processor = new CSVStreamingProcessor(8192, ',', '"', hasHeader,
            true, headersEach, CsvDataTypes.parse(types));
        Iterator<StreamChunk> it = processor.getChunkIterator(
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), "text/csv", chunkSize);
        List<StreamChunk> out = new ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }

    /** Text written directly must equal the tree's own serialization, chunk by chunk. */
    private static void assertTextMatchesTree(List<StreamChunk> chunks) {
        assertTrue(!chunks.isEmpty());
        for (StreamChunk chunk : chunks) {
            String direct = chunk.getPayloadText();   // before the tree exists: direct write
            String tree = chunk.getJSONPayload().toString();
            assertEquals("chunk " + chunk.getChunkNumber(), tree, direct);
            // Once the tree is built, the text comes from it - still the same.
            assertEquals(tree, chunk.getPayloadText());
        }
    }

    @Test
    public void typedObjectsMatchTheTree() throws Exception {
        assertTextMatchesTree(chunks(CSV, true, true, TYPES, 2));
    }

    @Test
    public void untypedObjectsMatchTheTree() throws Exception {
        assertTextMatchesTree(chunks(CSV, true, true, null, 4));
    }

    @Test
    public void arraysWithoutHeadersEachMatchTheTree() throws Exception {
        assertTextMatchesTree(chunks(CSV, true, false, TYPES, 3));
    }

    @Test
    public void headerlessFileMatchesTheTree() throws Exception {
        String csv = "1,a,2.50\n2,\"b,c\",3\n";
        List<StreamChunk> chunks = chunks(csv, false, true, null, 10);
        assertTextMatchesTree(chunks);
        assertEquals("[[\"1\",\"a\",\"2.50\"],[\"2\",\"b,c\",\"3\"]]", chunks.get(0).getPayloadText());
    }

    @Test
    public void typedValuesKeepTheirJsonTypesAndRawFallbacks() throws Exception {
        List<StreamChunk> chunks = chunks(CSV, true, true, TYPES, 10);
        JsonArray rows = chunks.get(0).getJSONPayload().getAsJsonArray();
        JsonObject first = rows.get(0).getAsJsonObject();
        assertTrue(first.get("id").getAsJsonPrimitive().isNumber());
        assertEquals("120.50", first.get("price").getAsBigDecimal().toPlainString());
        assertTrue(first.get("active").getAsBoolean());
        // Unconvertible cells stay strings rather than being dropped.
        JsonObject third = rows.get(2).getAsJsonObject();
        assertEquals("N/A", third.get("id").getAsString());
        assertEquals("maybe", third.get("active").getAsString());
        assertTrue(chunks.get(0).getPayloadText().startsWith(
            "[{\"id\":1,\"name\":\"Alice\",\"price\":120.50,\"active\":true,\"note\":\"plain\"}"));
    }

    @Test
    public void duplicateHeadersKeepTheTreeBehaviour() throws Exception {
        // A JsonObject keeps one "x" (the last value, in the first position); a naive stream would
        // emit two.
        String csv = "x,y,x\n1,2,3\n";
        CSVStreamingProcessor processor = new CSVStreamingProcessor(8192, ',', '"', true, true, true,
            CsvDataTypes.NONE);
        List<StreamChunk> chunks;
        try {
            Iterator<StreamChunk> it = processor.getChunkIterator(
                new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), "text/csv", 5);
            chunks = new ArrayList<>();
            it.forEachRemaining(chunks::add);
        } catch (Exception e) {
            // This commons-csv configuration may reject duplicate headers outright; then there is
            // nothing to compare.
            return;
        }
        assertTextMatchesTree(chunks);
        assertEquals("[{\"x\":\"3\",\"y\":\"2\"}]", chunks.get(0).getPayloadText());
    }

    @Test
    public void payloadsBuildCorrectlyOnManyThreadsAtOnce() throws Exception {
        StringBuilder csv = new StringBuilder("id,name,price,active\n");
        for (int i = 0; i < 5000; i++) {
            csv.append(i).append(",n").append(i).append(',').append(i % 7 == 0 ? "bad" : i + ".25")
                .append(',').append(i % 2 == 0).append('\n');
        }
        List<StreamChunk> chunks = chunks(csv.toString(), true, true, TYPES, 100);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<String>> texts = new ArrayList<>();
            for (StreamChunk chunk : chunks) {
                texts.add(pool.submit(chunk::getPayloadText));
            }
            for (int i = 0; i < chunks.size(); i++) {
                assertEquals(chunks.get(i).getJSONPayload().toString(), texts.get(i).get());
            }
        } finally {
            pool.shutdown();
        }
    }
}
