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
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.carbon.inbound.streaming.StreamChunk;
import org.wso2.carbon.inbound.streaming.StreamRecord;

import java.io.File;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Integration tests reading actual CSV files from the file system.
 */
public class CSVStreamingIntegrationTest {

    private static final String TEST_RESOURCES_DIR = "src/test/resources";

    private File getTestFile(String filename) {
        String filepath = TEST_RESOURCES_DIR + File.separator + filename;
        File file = new File(filepath);
        Assert.assertTrue("Test file not found: " + filepath, file.exists());
        return file;
    }

    @Test
    public void testReadSampleCSVRowByRow() throws Exception {
        File csvFile = getTestFile("sample.csv");

        // Row-by-row mode with metadata
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, true, false
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            // Use getRecordIterator() for row-by-row mode
            Iterator<StreamRecord> iterator = processor.getRecordIterator(input, "text/csv");

            int recordCount = 0;

            while (iterator.hasNext()) {
                StreamRecord record = iterator.next();
                recordCount++;

                Assert.assertTrue("Record should be valid", record.isValid());

                // Record mode with headers: payload is a JSON object keyed by header names.
                JsonObject payload = record.getJSONPayload().getAsJsonObject();
                Assert.assertNotNull("Payload should be present", payload);

                if (recordCount == 10) {
                    Assert.assertEquals("10", payload.get("ID").getAsString());
                    Assert.assertEquals("Henry Taylor", payload.get("Name").getAsString());
                    Assert.assertEquals("henry.taylor@example.com", payload.get("Email").getAsString());
                    Assert.assertEquals("Sales", payload.get("Department").getAsString());
                    Assert.assertEquals("51000", payload.get("Salary").getAsString());
                }
            }

            // Sample CSV has 10 data rows
            Assert.assertEquals("Should have read 10 data rows", 10, recordCount);
        }
    }

    @Test
    public void testReadSampleCSVInBatchMode() throws Exception {
        File csvFile = getTestFile("sample.csv");

        // Batch mode: 3 rows per chunk with metadata. addHeadersToEachResult=true selects the
        // array-of-objects payload shape (keyed by header names) in chunk mode.
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, true, true
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamChunk> iterator = processor.getChunkIterator(input, "text/csv", 3);

            int chunkCount = 0;
            int totalRowsRead = 0;

            while (iterator.hasNext()) {
                StreamChunk chunk = iterator.next();
                chunkCount++;

                Assert.assertTrue("Chunk should be valid", chunk.isValid());

                // Each chunk should have up to 3 records
                int recordsInChunk = chunk.getRecordCount();
                if (chunkCount < 4) {
                    Assert.assertEquals("Chunk should have 3 records", 3, recordsInChunk);
                } else {
                    Assert.assertEquals("Last chunk should have 1 record", 1, recordsInChunk);
                }
                // Verify records in chunk
                List<StreamRecord> records = chunk.getRecords();
                Assert.assertEquals("Number of records should match recordCount", recordsInChunk, records.size());

                if (chunkCount == 2) {
                    JsonArray payload = chunk.getJSONPayload().getAsJsonArray();
                    JsonObject fifthRecord = payload.get(1).getAsJsonObject();
                    Assert.assertEquals("5", fifthRecord.get("ID").getAsString());
                    Assert.assertEquals("Charlie Brown", fifthRecord.get("Name").getAsString());
                    Assert.assertEquals("charlie.brown@example.com", fifthRecord.get("Email").getAsString());
                    Assert.assertEquals("Sales", fifthRecord.get("Department").getAsString());
                    Assert.assertEquals("48000", fifthRecord.get("Salary").getAsString());
                }

                // Verify each record is valid
                for (StreamRecord record : records) {
                    Assert.assertTrue("Record should be valid", record.isValid());
                }

                totalRowsRead += recordsInChunk;
            }

            // 10 data rows / 3 rows per chunk = 3 full chunks + 1 partial chunk
            Assert.assertEquals("Should have 4 chunks in batch mode (3+3+3+1)", 4, chunkCount);
            Assert.assertEquals("Should have read 10 data rows total", 10, totalRowsRead);
        }
    }

    @Test
    public void testReadSampleCSVRowByRowWithoutVariableOutput() throws Exception {
        File csvFile = getTestFile("sample.csv");

        // Row-by-row mode, raw content, addHeadersToEachResult = true
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, false, true
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            // Use getRecordIterator() for row-by-row mode
            Iterator<StreamRecord> iterator = processor.getRecordIterator(input, "text/csv");

            StreamRecord record = iterator.next();
            String payload = new String(record.getContent(), record.getEncoding());
            Assert.assertNotNull("Payload should not be null", payload);
            // Headers should be prepended to the data row
            Assert.assertEquals("ID,Name,Email,Department,Salary" + System.lineSeparator()
                + "1,John Doe,john.doe@example.com,Sales,50000", payload);
        }
    }

    @Test
    public void testReadSampleCSVRowByRowWithoutHeadersInEachRecord() throws Exception {
        File csvFile = getTestFile("sample.csv");

        // Row-by-row mode, raw content, addHeadersToEachResult = false
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, false, false
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            // Use getRecordIterator() for row-by-row mode
            Iterator<StreamRecord> iterator = processor.getRecordIterator(input, "text/csv");

            StreamRecord record = iterator.next();
            String payload = new String(record.getContent(), record.getEncoding());
            Assert.assertNotNull("Payload should not be null", payload);
            // Headers should NOT be prepended - only the data row is present
            Assert.assertEquals("1,John Doe,john.doe@example.com,Sales,50000", payload);
        }
    }

    @Test
    public void testReadQuotedCSVRowByRow() throws Exception {
        File csvFile = getTestFile("sample-quoted.csv");

        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, true, false
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            // Use getRecordIterator() for row-by-row mode
            Iterator<StreamRecord> iterator = processor.getRecordIterator(input, "text/csv");

            int recordCount = 0;
            while (iterator.hasNext()) {
                StreamRecord record = iterator.next();
                recordCount++;

                Assert.assertTrue("Record should be valid", record.isValid());

                // Record mode with headers: payload is a JSON object keyed by header names.
                JsonObject payload = record.getJSONPayload().getAsJsonObject();
                Assert.assertNotNull("Payload should be present", payload);

                // Verify quoted fields with embedded commas are parsed as single fields
                if (recordCount == 1) {
                    Assert.assertEquals("1", payload.get("ID").getAsString());
                    Assert.assertEquals("Smith, John", payload.get("Name").getAsString());
                    Assert.assertEquals("123 Main St, Apt 4", payload.get("Address").getAsString());
                    Assert.assertEquals("555-0101", payload.get("Phone").getAsString());
                } else if (recordCount == 5) {
                    Assert.assertEquals("5", payload.get("ID").getAsString());
                    Assert.assertEquals("Brown, Charlie", payload.get("Name").getAsString());
                    Assert.assertEquals("654 Maple Dr, Unit B", payload.get("Address").getAsString());
                    Assert.assertEquals("555-0105", payload.get("Phone").getAsString());
                }
            }

            // sample-quoted.csv has 5 data rows
            Assert.assertEquals("Should have 5 records", 5, recordCount);
        }
    }

    @Test
    public void testReadQuotedCSVInBatchMode() throws Exception {
        File csvFile = getTestFile("sample-quoted.csv");

        // addHeadersToEachResult=true selects the array-of-objects payload shape in chunk mode.
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, true, true
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamChunk> iterator = processor.getChunkIterator(input, "text/csv", 5);

            int chunkCount = 0;
            int totalRows = 0;

            while (iterator.hasNext()) {
                StreamChunk chunk = iterator.next();
                chunkCount++;

                List<StreamRecord> records = chunk.getRecords();
                for (StreamRecord record : records) {
                    Assert.assertTrue("Record should be valid", record.isValid());
                    totalRows++;
                }

                // Verify quoted fields with embedded commas are parsed correctly
                JsonArray payload = chunk.getJSONPayload().getAsJsonArray();
                Assert.assertNotNull("Payload should be present", payload);
                Assert.assertEquals("Payload should have 5 records", 5, payload.size());

                JsonObject secondRecord = payload.get(1).getAsJsonObject();
                Assert.assertEquals("2", secondRecord.get("ID").getAsString());
                Assert.assertEquals("Doe, Jane", secondRecord.get("Name").getAsString());
                Assert.assertEquals("456 Oak Ave, Suite 200", secondRecord.get("Address").getAsString());
                Assert.assertEquals("555-0102", secondRecord.get("Phone").getAsString());

                JsonObject thirdRecord = payload.get(2).getAsJsonObject();
                Assert.assertEquals("3", thirdRecord.get("ID").getAsString());
                Assert.assertEquals("Johnson, Bob", thirdRecord.get("Name").getAsString());
                Assert.assertEquals("789 Pine Rd, Building A", thirdRecord.get("Address").getAsString());
                Assert.assertEquals("555-0103", thirdRecord.get("Phone").getAsString());
            }

            // 5 rows / 5 per chunk = 1 chunk
            Assert.assertEquals("Should have 1 chunk", 1, chunkCount);
            Assert.assertEquals("Should have 5 total rows", 5, totalRows);
        }
    }

    @Test
    public void testReadQuotedCSVRowByRowRawContent() throws Exception {
        File csvFile = getTestFile("sample-quoted.csv");

        // Raw content mode - quoted fields with embedded commas should be preserved
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, false, false
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamRecord> iterator = processor.getRecordIterator(input, "text/csv");

            // Fields containing the delimiter are re-quoted per RFC 4180, so the raw content
            // is a faithful round-trip: "Smith, John" and "123 Main St, Apt 4" keep their quotes.
            StreamRecord record = iterator.next();
            String payload = new String(record.getContent(), record.getEncoding());
            Assert.assertEquals("1,\"Smith, John\",\"123 Main St, Apt 4\",555-0101", payload);
        }
    }

    @Test
    public void testRowByRowModeWithRawContent() throws Exception {
        File csvFile = getTestFile("sample.csv");

        // Row-by-row mode with raw content (no metadata)
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, false, false
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamChunk> iterator = processor.getChunkIterator(input, "text/csv", 1);

            int recordCount = 0;
            while (iterator.hasNext()) {
                StreamChunk chunk = iterator.next();
                StreamRecord record = chunk.getRecords().get(0);

                // Content should be populated, not metadata
                Assert.assertNotNull("Content should not be null", record.getContent());
                Assert.assertTrue("Content should not be empty", record.getContent().length > 0);
                recordCount++;
            }

            Assert.assertEquals("Should read 10 records", 10, recordCount);
        }
    }

    @Test
    public void testRowByRowModeWithHeadersInContent() throws Exception {
        File csvFile = getTestFile("sample.csv");

        // Row-by-row mode with headers prepended to content
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, false, true
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            // Use getRecordIterator() for row-by-row mode
            Iterator<StreamRecord> iterator = processor.getRecordIterator(input, "text/csv");

            // First record should have headers prepended
            StreamRecord record1 = iterator.next();
            String content1 = record1.getAsString();

            // Content should contain headers
            String[] lines = content1.split(System.lineSeparator());
            Assert.assertEquals("Should have 2 lines (header + content)", 2, lines.length);
            // First line should be headers
            Assert.assertTrue("First line should contain column names", lines[0].contains("ID"));
        }
    }


    @Test
    public void testRecordNumberTracking() throws Exception {
        File csvFile = getTestFile("sample.csv");

        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, false, false
        );

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamChunk> iterator = processor.getChunkIterator(input, "text/csv", 1);

            // The header is skipped, so data rows are numbered 1-based.
            StreamChunk chunk1 = iterator.next();
            StreamRecord record1 = chunk1.getRecords().get(0);
            Assert.assertEquals("First data row should have record number 1", 1, record1.getRecordNumber());

            // Second data row should have record number 2
            StreamChunk chunk2 = iterator.next();
            StreamRecord record2 = chunk2.getRecords().get(0);
            Assert.assertEquals("Second data row should have record number 2", 2, record2.getRecordNumber());
        }
    }

    // ----- Column data types -----

    private static final String SALARY_AND_ID_TYPES =
        "[{\"Column Name Or Index\":\"ID\",\"Is Column Name\":\"Yes\",\"Data Type\":\"Integer\"},"
        + "{\"Column Name Or Index\":\"5\",\"Is Column Name\":\"No\",\"Data Type\":\"Number\"}]";

    @Test
    public void testRecordModeAppliesColumnDataTypes() throws Exception {
        File csvFile = getTestFile("sample.csv");
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, true, false, CsvDataTypes.parse(SALARY_AND_ID_TYPES));

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamRecord> iterator = processor.getRecordIterator(input, "text/csv");
            JsonObject payload = iterator.next().getJSONPayload().getAsJsonObject();

            Assert.assertTrue("ID is typed by name", payload.get("ID").getAsJsonPrimitive().isNumber());
            Assert.assertEquals(1L, payload.get("ID").getAsLong());
            Assert.assertTrue("Salary is typed by 1-based index",
                payload.get("Salary").getAsJsonPrimitive().isNumber());
            Assert.assertEquals("50000", payload.get("Salary").getAsString());
            Assert.assertTrue("Untyped columns stay strings",
                payload.get("Name").getAsJsonPrimitive().isString());
            Assert.assertEquals("{\"ID\":1,\"Name\":\"John Doe\","
                + "\"Email\":\"john.doe@example.com\",\"Department\":\"Sales\","
                + "\"Salary\":50000}", payload.toString());
        }
    }

    @Test
    public void testChunkModeAppliesColumnDataTypes() throws Exception {
        File csvFile = getTestFile("sample.csv");
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, true, true, CsvDataTypes.parse(SALARY_AND_ID_TYPES));

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamChunk> iterator = processor.getChunkIterator(input, "text/csv", 3);
            JsonArray payload = iterator.next().getJSONPayload().getAsJsonArray();

            Assert.assertEquals(3, payload.size());
            JsonObject first = payload.get(0).getAsJsonObject();
            Assert.assertTrue(first.get("ID").getAsJsonPrimitive().isNumber());
            Assert.assertTrue(first.get("Salary").getAsJsonPrimitive().isNumber());
            Assert.assertTrue(first.get("Department").getAsJsonPrimitive().isString());
            // Every row in the chunk is converted, not just the first.
            Assert.assertEquals(3L, payload.get(2).getAsJsonObject().get("ID").getAsLong());
        }
    }

    @Test
    public void testChunkModeWithoutHeadersAppliesIndexRules() throws Exception {
        File csvFile = getTestFile("sample.csv");
        // addHeadersToEachResult=false: rows are emitted as arrays, indexes still apply.
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, true, false, CsvDataTypes.parse(SALARY_AND_ID_TYPES));

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamChunk> iterator = processor.getChunkIterator(input, "text/csv", 1);
            JsonArray row = iterator.next().getJSONPayload().getAsJsonArray()
                .get(0).getAsJsonArray();

            Assert.assertTrue(row.get(0).getAsJsonPrimitive().isNumber());
            Assert.assertTrue(row.get(1).getAsJsonPrimitive().isString());
            Assert.assertTrue(row.get(4).getAsJsonPrimitive().isNumber());
        }
    }

    @Test
    public void testWithoutDataTypesEverythingStaysAString() throws Exception {
        File csvFile = getTestFile("sample.csv");
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, true, false, CsvDataTypes.NONE);

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamRecord> iterator = processor.getRecordIterator(input, "text/csv");
            JsonObject payload = iterator.next().getJSONPayload().getAsJsonObject();
            Assert.assertTrue(payload.get("ID").getAsJsonPrimitive().isString());
            Assert.assertTrue(payload.get("Salary").getAsJsonPrimitive().isString());
        }
    }

    @Test
    public void testDataTypesDoNotAffectRawContentMode() throws Exception {
        File csvFile = getTestFile("sample.csv");
        // addOutputToProperty=false: the record carries the original CSV text, which has no types.
        CSVStreamingProcessor processor = new CSVStreamingProcessor(
            8192, ',', '"', true, false, false, CsvDataTypes.parse(SALARY_AND_ID_TYPES));

        try (InputStream input = Files.newInputStream(csvFile.toPath())) {
            Iterator<StreamRecord> iterator = processor.getRecordIterator(input, "text/csv");
            StreamRecord record = iterator.next();
            Assert.assertEquals("1,John Doe,john.doe@example.com,Sales,50000",
                new String(record.getContent(), record.getEncoding()));
        }
    }
}
