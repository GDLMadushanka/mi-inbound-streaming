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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.streaming.ChunkedDataProcessor;
import org.wso2.carbon.inbound.streaming.StreamChunk;
import org.wso2.carbon.inbound.streaming.StreamRecord;
import org.wso2.carbon.inbound.streaming.StreamingException;

public class CSVStreamingProcessor extends ChunkedDataProcessor {

    private static final Log log = LogFactory.getLog(CSVStreamingProcessor.class);

    private final char delimiter;
    private final char quoteChar;
    private final boolean hasHeader;
    private int chunkSize = 1;  // Number of rows per chunk in batch mode.
    private final boolean addOutputToProperty;  // Add output to variable (metadata) of body
    private final boolean addHeadersToEachResult; // Add header to each result row or chunk
    private final CsvDataTypes dataTypes; // Declared JSON type per column; never null

    public CSVStreamingProcessor(int bufferSize, char delimiter, char quoteChar, boolean hasHeader,
        boolean addOutputToProperty, boolean addHeadersToEachResult) {
        this(bufferSize, delimiter, quoteChar, hasHeader, addOutputToProperty,
            addHeadersToEachResult, CsvDataTypes.NONE);
    }

    public CSVStreamingProcessor(int bufferSize, char delimiter, char quoteChar, boolean hasHeader,
        boolean addOutputToProperty, boolean addHeadersToEachResult, CsvDataTypes dataTypes) {
        super(bufferSize);
        this.delimiter = delimiter;
        this.quoteChar = quoteChar;
        this.hasHeader = hasHeader;
        this.addOutputToProperty = addOutputToProperty;
        this.addHeadersToEachResult = addHeadersToEachResult;
        this.dataTypes = dataTypes != null ? dataTypes : CsvDataTypes.NONE;
    }

    @Override
    public Iterator<StreamChunk> getChunkIterator(InputStream input, String contentType,
        int chunkSize, long startFromRecord, long startFromChunk)
        throws StreamingException {
        this.chunkSize = Math.max(1, chunkSize);
        try {
            Charset charset = detectCharset(contentType);
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, charset),
                bufferSize
            );

            return new ChunkIterator(reader, charset, startFromRecord, startFromChunk);

        } catch (Exception e) {
            throw new StreamingException("Failed to initialize CSV processor", e, 0, false);
        }
    }

    @Override
    public Iterator<StreamRecord> getRecordIterator(InputStream input, String contentType,
        long startFromRecord)
        throws StreamingException {
        try {
            Charset charset = detectCharset(contentType);
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, charset),
                bufferSize
            );

            return new RowIterator(reader, charset, startFromRecord);

        } catch (Exception e) {
            throw new StreamingException("Failed to initialize CSV processor", e, 0, false);
        }
    }

    /**
     * Build CSVFormat with configured delimiter, quote character, and header handling.
     */
    private CSVFormat buildCSVFormat() {
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder()
            .setDelimiter(delimiter)
            .setQuote(quoteChar)
            .setIgnoreEmptyLines(true)
            .setTrim(false);

        if (hasHeader) {
            builder = builder
                .setHeader()
                .setSkipHeaderRecord(true);
        }

        return builder.get();
    }

    /**
     * Build record content as a delimiter-separated string. Fields are re-quoted per RFC 4180 so
     * the reconstruction is a faithful, non-lossy round-trip: a field is wrapped in quotes if it
     * contains the delimiter, the quote character, or a line break, and any embedded quote
     * characters are doubled.
     */
    private String buildRecordContent(CSVRecord record) {
        if (record.size() == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(quoteField(record.get(0)));
        for (int i = 1; i < record.size(); i++) {
            sb.append(delimiter).append(quoteField(record.get(i)));
        }
        return sb.toString();
    }

    /**
     * Quote a single CSV field per RFC 4180 if it contains the delimiter, the quote character, or a
     * line break. Embedded quote characters are escaped by doubling.
     */
    private String quoteField(String field) {
        boolean needsQuoting = field.indexOf(delimiter) >= 0
            || field.indexOf(quoteChar) >= 0
            || field.indexOf('\n') >= 0
            || field.indexOf('\r') >= 0;

        if (!needsQuoting) {
            return field;
        }

        String escaped = field.replace(String.valueOf(quoteChar),
            String.valueOf(quoteChar) + quoteChar);
        return quoteChar + escaped + quoteChar;
    }

    /**
     * Iterator for batch mode: yields chunks containing multiple records. Guarantees complete,
     * valid records without split fields or partial rows. Each chunk contains up to chunkSize
     * rows.
     */
    private class ChunkIterator implements Iterator<StreamChunk> {

        private final Iterator<CSVRecord> csvIterator;
        private String[] headers;
        // Whether the header row repeats a name; decides how a deferred chunk payload is written.
        private boolean duplicateHeaders;
        // Column rules resolved against this file's header row, once, in the constructor.
        private CsvDataTypes.Converter converter;
        private long recordCount = 0;
        private int chunkNumber = 0;
        private boolean eof = false;
        private CSVRecord nextRecord;
        private final Charset charset;

        ChunkIterator(BufferedReader reader, Charset charset, long startFromRecord,
                long startFromChunk)
            throws StreamingException {
            this.charset = charset;
            // Carry on numbering chunks from where the checkpoint left off.
            this.chunkNumber = (int) startFromChunk;
            try {
                CSVFormat csvFormat = buildCSVFormat();
                CSVParser csvParser = csvFormat.parse(reader);
                this.csvIterator = csvParser.iterator();

                if (hasHeader) {
                    this.headers = csvParser.getHeaderNames().toArray(new String[0]);
                    if (log.isDebugEnabled()) {
                        log.debug("CSV headers parsed: " + csvParser.getHeaderNames().size()
                            + " columns");
                    }
                }
                this.converter = dataTypes.resolve(this.headers);
                this.duplicateHeaders = headers != null
                    && new HashSet<>(Arrays.asList(headers)).size() < headers.length;

                // Try to read first record
                advanceToNextRecord();
                // Resume: the header (row 0) is already parsed above; discard the leading data
                // rows already processed before the checkpoint.
                while (recordCount < startFromRecord && hasNext()) {
                    recordCount++;
                    advanceToNextRecord();
                }

            } catch (IOException e) {
                throw new StreamingException("Failed to parse CSV headers", e, 0, false,
                    StreamingException.Kind.PARSE);
            }
        }

        private void advanceToNextRecord() {
            try {
                if (csvIterator.hasNext()) {
                    nextRecord = csvIterator.next();
                } else {
                    nextRecord = null;
                    eof = true;
                }
            } catch (UncheckedIOException ex) {
                long errorRecordNumber = recordCount + 1;
                if (hasHeader) errorRecordNumber++;
                throw new StreamingException("Error reading CSV record", ex.getCause(),
                    errorRecordNumber, false, StreamingException.Kind.PARSE);
            }
        }

        @Override
        public boolean hasNext() {
            return !eof && nextRecord != null;
        }

        @Override
        public StreamChunk next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more records to iterate");
            }
            return getNextBatchChunk();
        }

        /**
         * Get next chunk in batch mode. Each StreamChunk contains up to chunkSize rows. Headers are
         * stored on the chunk, not on individual records (unless configured).
         */
        private StreamChunk getNextBatchChunk() {
            chunkNumber++;
            StreamChunk chunk = new StreamChunk(chunkSize);
            chunk.setChunkNumber(chunkNumber);
            chunk.setEncoding(charset);

            // Property-output mode: keep only the tokenized cells here. Converting them and
            // building the JSON is deferred to whoever asks for the payload (a worker, when the
            // file is mediated in parallel), which keeps this - the one thread reading the file -
            // down to tokenizing.
            List<String[]> rows = null;
            if (addOutputToProperty) {
                rows = new ArrayList<>(chunkSize);
                chunk.setDeferredPayload(new CsvChunkPayload(headers, rows, converter,
                    addHeadersToEachResult && headers != null && headers.length > 0,
                    duplicateHeaders));
            } else {
                // adding header as first record in the chunk if configured to do so
                if (addHeadersToEachResult && headers != null && headers.length > 0) {
                    StreamRecord streamRecord = getStreamRecord();
                    chunk.addRecord(streamRecord);
                }
            }

            int rowsInChunk = 0;
            while (rowsInChunk < chunkSize && hasNext()) {
                recordCount++;
                CSVRecord record = nextRecord;
                advanceToNextRecord();
                rowsInChunk++;

                // Create StreamRecord for this row
                StreamRecord streamRecord = new StreamRecord(recordCount);
                streamRecord.setEncoding(charset);
                streamRecord.setValid(true);

                if (addOutputToProperty) {
                    rows.add(record.values());
                } else {
                    String recordContent = buildRecordContent(record);
                    streamRecord.setContent(recordContent.getBytes(charset));
                }

                chunk.addRecord(streamRecord);
            }

            chunk.setRecordCount(rowsInChunk);
            chunk.setLastChunk(!hasNext());

            if (log.isDebugEnabled()) {
                log.debug("Batch mode: Chunk with " + rowsInChunk + " rows (rows " +
                    chunk.getFirstRecordNumber() + "-" + chunk.getLastRecordNumber() + ")");
            }

            return chunk;
        }

        private StreamRecord getStreamRecord() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < headers.length; i++) {
                sb.append(headers[i]);
                if (i < headers.length - 1) {
                    sb.append(delimiter);
                }
            }
            StreamRecord streamRecord = new StreamRecord(recordCount);
            streamRecord.setEncoding(charset);
            streamRecord.setValid(true);
            streamRecord.setContent(sb.toString().getBytes(charset));
            return streamRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }

    /**
     * Iterator for row-by-row mode: yields individual StreamRecords. Guarantees complete, valid
     * records without split fields or partial rows. Each iteration returns a single StreamRecord.
     */
    private class RowIterator implements Iterator<StreamRecord> {

        private final Charset charset;
        private final Iterator<CSVRecord> csvIterator;
        private String[] headers;
        // Column rules resolved against this file's header row, once, in the constructor.
        private CsvDataTypes.Converter converter;
        private long recordCount = 0;
        private boolean eof = false;
        private CSVRecord nextRecord;

        RowIterator(BufferedReader reader, Charset charset, long startFromRecord)
            throws StreamingException {
            this.charset = charset;

            try {
                CSVFormat csvFormat = buildCSVFormat();
                CSVParser csvParser = csvFormat.parse(reader);
                this.csvIterator = csvParser.iterator();

                if (hasHeader) {
                    this.headers = csvParser.getHeaderNames().toArray(new String[0]);
                    if (log.isDebugEnabled()) {
                        log.debug("CSV headers parsed: " + csvParser.getHeaderNames().size()
                            + " columns");
                    }
                }
                this.converter = dataTypes.resolve(this.headers);

                advanceToNextRecord();
                // Resume: the header (row 0) is already parsed above; discard the leading data
                // rows already processed before the checkpoint.
                while (recordCount < startFromRecord && hasNext()) {
                    recordCount++;
                    advanceToNextRecord();
                }

            } catch (IOException e) {
                throw new StreamingException("Failed to parse CSV headers", e, 0, false,
                    StreamingException.Kind.PARSE);
            }
        }

        private void advanceToNextRecord() {
            try {
                if (csvIterator.hasNext()) {
                    nextRecord = csvIterator.next();
                } else {
                    nextRecord = null;
                    eof = true;
                }
            } catch (UncheckedIOException ex) {
                long errorRecordNumber = recordCount + 1;
                if (hasHeader) errorRecordNumber++;
                throw new StreamingException("Error reading CSV record", ex.getCause(),
                    errorRecordNumber, false, StreamingException.Kind.PARSE);
            }
        }

        @Override
        public boolean hasNext() {
            return !eof && nextRecord != null;
        }

        @Override
        public StreamRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more records to iterate");
            }

            recordCount++;
            CSVRecord record = nextRecord;
            advanceToNextRecord();

            try {
                StreamRecord streamRecord = new StreamRecord(recordCount);
                streamRecord.setEncoding(charset);
                streamRecord.setValid(true);

                if (addOutputToProperty) {
                    if (headers != null && headers.length > 0) {
                        JsonObject jsonObject = new JsonObject();
                        for (int i = 0; i < headers.length && i < record.size(); i++) {
                            jsonObject.add(headers[i], converter.convert(i, record.get(i)));
                        }
                        // {"ID":1,"Name":"John"} - ID is a number only if it is typed as one
                        streamRecord.setJSONPayload(jsonObject);
                    } else {
                        // If no headers, store the row as a list of values
                        JsonArray jsonArray = new JsonArray();
                        for (int i = 0; i < record.size(); i++) {
                            jsonArray.add(converter.convert(i, record.get(i)));
                        }
                        // [1,"John"]
                        streamRecord.setJSONPayload(jsonArray);
                    }
                } else {
                    String recordContent = buildRecordContent(record);
                    if (addHeadersToEachResult && headers != null && headers.length > 0) {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < headers.length; i++) {
                            sb.append(headers[i]);
                            if (i < headers.length - 1) {
                                sb.append(delimiter);
                            }
                        }
                        sb.append(System.lineSeparator());
                        sb.append(recordContent);
                        recordContent = sb.toString();
                    }
                    streamRecord.setContent(recordContent.getBytes(charset));
                }

                return streamRecord;

            } catch (Exception e) {
                StreamRecord errorRecord = new StreamRecord(recordCount);
                errorRecord.setValid(false);
                errorRecord.setParseError("CSV parsing error: " + e.getMessage());

                if (log.isWarnEnabled()) {
                    log.warn("CSV parsing error at record " + errorRecord.getRecordNumber(), e);
                }
                return errorRecord;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }
}
