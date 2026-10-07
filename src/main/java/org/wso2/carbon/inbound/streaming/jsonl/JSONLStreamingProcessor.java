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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.streaming.Utils;
import org.wso2.carbon.inbound.streaming.ChunkedDataProcessor;
import org.wso2.carbon.inbound.streaming.StreamChunk;
import org.wso2.carbon.inbound.streaming.StreamRecord;
import org.wso2.carbon.inbound.streaming.StreamingException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Streaming processor for JSON Lines (JSONL / NDJSON) input, where every line is an independent
 * JSON value.
 * <p>
 * Unlike CSV or a single JSON document, a line that fails to parse does <em>not</em> render the
 * rest of the file meaningless. Such a line is emitted as an <b>invalid</b> {@link StreamRecord}
 * (its raw bytes preserved in {@link StreamRecord#getContent()}, {@link StreamRecord#isValid()}
 * false and {@link StreamRecord#getParseError()} set) - i.e. a recoverable, per-record failure that
 * the caller can siphon off and skip. Only an underlying I/O read failure is non-recoverable and is
 * surfaced as a {@link StreamingException} with {@code isRecoverable == false}, aborting the file.
 * <p>
 * Blank lines are skipped. When {@code addOutputToProperty} is enabled each parsed line is exposed
 * under the "payload" key as a plain object; otherwise the raw line bytes become the record content.
 */
public class JSONLStreamingProcessor extends ChunkedDataProcessor {

    private static final Log log = LogFactory.getLog(JSONLStreamingProcessor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final boolean addOutputToProperty;

    public JSONLStreamingProcessor(int bufferSize, boolean addOutputToProperty) {
        super(bufferSize);
        this.addOutputToProperty = addOutputToProperty;
    }

    @Override
    public Iterator<StreamChunk> getChunkIterator(InputStream input, String contentType, int chunkSize,
            long startFromRecord, long startFromChunk) throws StreamingException {
        try {
            Charset charset = detectCharset(contentType);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(input, charset), bufferSize);
            return new JsonlChunkIterator(reader, charset, Math.max(1, chunkSize), startFromRecord,
                    startFromChunk);
        } catch (Exception e) {
            throw new StreamingException("Failed to initialize JSONL processor", e, 0, false);
        }
    }

    @Override
    public Iterator<StreamRecord> getRecordIterator(InputStream input, String contentType,
            long startFromRecord) throws StreamingException {
        try {
            Charset charset = detectCharset(contentType);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(input, charset), bufferSize);
            return new JsonlRowIterator(reader, charset, startFromRecord);
        } catch (Exception e) {
            throw new StreamingException("Failed to initialize JSONL processor", e, 0, false);
        }
    }

    /**
     * A JSONL chunk body is a JSON array of the lines (each line is a compact JSON value), so a
     * multi-record chunk is valid application/json rather than newline-delimited JSONL.
     */
    @Override
    public byte[] buildChunkBody(StreamChunk chunk) {
        return buildJsonArrayChunkBody(chunk);
    }

    /**
     * Build a StreamRecord from a single raw line, parsing it as JSON. A parse failure yields an
     * invalid record (recoverable); the raw bytes are always retained so the caller can siphon the
     * line verbatim.
     */
    private StreamRecord toRecord(String line, long recordNumber, Charset charset) {
        StreamRecord record = new StreamRecord(recordNumber);
        record.setEncoding(charset);
        try {
            JsonNode node = MAPPER.readTree(line);
            record.setValid(true);
            if (addOutputToProperty) {
                record.setJSONPayload(Utils.convertJacksonToGson(node));
            } else {
                record.setContent(line.getBytes(charset));
            }
        } catch (JsonProcessingException e) {
            // Recoverable, per-line failure: keep the raw line so it can be siphoned and skipped.
            record.setValid(false);
            record.setParseError(e.getOriginalMessage());
            record.setContent(line.getBytes(charset));
            if (log.isDebugEnabled()) {
                log.debug("Malformed JSONL at line " + recordNumber + ": " + e.getOriginalMessage());
            }
        }
        return record;
    }

    /**
     * Reads the next non-blank line, or null at end of stream. Throws a non-recoverable
     * StreamingException on an underlying I/O failure.
     */
    private static String nextNonBlankLine(BufferedReader reader, long recordCount) {
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    return line;
                }
            }
            return null;
        } catch (IOException e) {
            throw new StreamingException(
                    "Failed to read JSONL stream after line " + recordCount, e, recordCount, false);
        }
    }

    /**
     * Record mode: one StreamRecord per line (valid or invalid).
     */
    private final class JsonlRowIterator implements Iterator<StreamRecord> {

        private final BufferedReader reader;
        private final Charset charset;
        private long recordCount = 0;
        private String nextLine;

        JsonlRowIterator(BufferedReader reader, Charset charset, long startFromRecord) {
            this.reader = reader;
            this.charset = charset;
            this.nextLine = nextNonBlankLine(reader, recordCount);
            // Resume: discard the leading records already processed before the checkpoint.
            while (recordCount < startFromRecord && nextLine != null) {
                recordCount++;
                this.nextLine = nextNonBlankLine(reader, recordCount);
            }
        }

        @Override
        public boolean hasNext() {
            return nextLine != null;
        }

        @Override
        public StreamRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more JSONL lines to iterate");
            }
            recordCount++;
            String line = nextLine;
            nextLine = nextNonBlankLine(reader, recordCount);
            return toRecord(line, recordCount, charset);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }

    /**
     * Chunk mode: up to chunkSize lines per StreamChunk. Invalid records are added to the chunk (so
     * the caller can siphon them) but never to the addOutputToProperty payload array.
     */
    private final class JsonlChunkIterator implements Iterator<StreamChunk> {

        private final BufferedReader reader;
        private final Charset charset;
        private final int chunkSize;
        private int chunkNumber = 0;
        private long recordCount = 0;
        private String nextLine;

        JsonlChunkIterator(BufferedReader reader, Charset charset, int chunkSize,
                long startFromRecord, long startFromChunk) {
            this.chunkNumber = (int) startFromChunk;
            this.reader = reader;
            this.charset = charset;
            this.chunkSize = chunkSize;
            this.nextLine = nextNonBlankLine(reader, recordCount);
            // Resume: discard the leading records already processed before the checkpoint.
            while (recordCount < startFromRecord && nextLine != null) {
                recordCount++;
                this.nextLine = nextNonBlankLine(reader, recordCount);
            }
        }

        @Override
        public boolean hasNext() {
            return nextLine != null;
        }

        @Override
        public StreamChunk next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more JSONL lines to iterate");
            }
            chunkNumber++;
            StreamChunk chunk = new StreamChunk(chunkSize);
            chunk.setChunkNumber(chunkNumber);
            chunk.setEncoding(charset);

            JsonArray resultsArray = null;
            if (addOutputToProperty) {
                resultsArray = new JsonArray();
                chunk.setJSONPayload(resultsArray);
            }

            int linesInChunk = 0;
            while (linesInChunk < chunkSize && hasNext()) {
                recordCount++;
                String line = nextLine;
                nextLine = nextNonBlankLine(reader, recordCount);
                linesInChunk++;

                StreamRecord record = toRecord(line, recordCount, charset);
                if (addOutputToProperty && record.isValid()) {
                    resultsArray.add(record.getJSONPayload());
                }
                chunk.addRecord(record);
            }

            chunk.setRecordCount(linesInChunk);
            chunk.setLastChunk(!hasNext());

            if (log.isDebugEnabled()) {
                log.debug("JSONL chunk with " + linesInChunk + " lines (lines "
                        + chunk.getFirstRecordNumber() + "-" + chunk.getLastRecordNumber() + ")");
            }
            return chunk;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }
}
