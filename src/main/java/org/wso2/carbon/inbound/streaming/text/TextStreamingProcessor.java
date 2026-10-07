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
import com.google.gson.JsonPrimitive;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.streaming.ChunkedDataProcessor;
import org.wso2.carbon.inbound.streaming.StreamChunk;
import org.wso2.carbon.inbound.streaming.StreamRecord;
import org.wso2.carbon.inbound.streaming.StreamingException;

/**
 * Streaming processor for plain-text inputs such as log files, where a record is a single line.
 * <p>
 * A {@link StreamRecord} represents one line and a {@link StreamChunk} groups a batch of lines.
 * Line terminators are stripped from each record; in raw-body chunk mode they are re-joined by
 * the caller. Blank lines are preserved as valid records (relevant for log files).
 * <p>
 * When {@code addOutputToProperty} is enabled the parsed output is exposed under the "payload"
 * metadata key: a single line string per record, or a list of line strings per chunk.
 */
public class TextStreamingProcessor extends ChunkedDataProcessor {

    private static final Log log = LogFactory.getLog(TextStreamingProcessor.class);

    private final boolean addOutputToProperty;

    public TextStreamingProcessor(int bufferSize, boolean addOutputToProperty) {
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
            return new TextChunkIterator(reader, charset, Math.max(1, chunkSize), startFromRecord,
                startFromChunk);
        } catch (Exception e) {
            throw new StreamingException("Failed to initialize text processor", e, 0, false);
        }
    }

    @Override
    public Iterator<StreamRecord> getRecordIterator(InputStream input, String contentType,
        long startFromRecord) throws StreamingException {
        try {
            Charset charset = detectCharset(contentType);
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, charset), bufferSize);
            return new TextRowIterator(reader, charset, startFromRecord);
        } catch (Exception e) {
            throw new StreamingException("Failed to initialize text processor", e, 0, false);
        }
    }

    /**
     * Iterator for record mode: yields one StreamRecord per line.
     */
    private class TextRowIterator implements Iterator<StreamRecord> {

        private final BufferedReader reader;
        private final Charset charset;
        private long recordCount = 0;
        private String nextLine;
        private boolean eof = false;

        TextRowIterator(BufferedReader reader, Charset charset, long startFromRecord) {
            this.reader = reader;
            this.charset = charset;
            advance();
            // Resume: discard the leading records already processed before the checkpoint.
            while (recordCount < startFromRecord && hasNext()) {
                recordCount++;
                advance();
            }
        }

        private void advance() {
            try {
                nextLine = reader.readLine();
                if (nextLine == null) {
                    eof = true;
                }
            } catch (IOException e) {
                nextLine = null;
                eof = true;
                log.warn("Error reading text stream at line " + recordCount, e);
            }
        }

        @Override
        public boolean hasNext() {
            return !eof && nextLine != null;
        }

        @Override
        public StreamRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more lines to iterate");
            }
            recordCount++;
            String line = nextLine;
            advance();

            StreamRecord record = new StreamRecord(recordCount);
            record.setEncoding(charset);
            record.setValid(true);

            if (addOutputToProperty) {
                record.setJSONPayload( new JsonPrimitive(line));
            } else {
                record.setContent(line.getBytes(charset));
            }
            return record;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }

    /**
     * Iterator for chunk mode: yields StreamChunks containing up to chunkSize lines.
     */
    private class TextChunkIterator implements Iterator<StreamChunk> {

        private final BufferedReader reader;
        private final Charset charset;
        private final int chunkSize;
        private int chunkNumber = 0;
        private long recordCount = 0;
        private String nextLine;
        private boolean eof = false;

        TextChunkIterator(BufferedReader reader, Charset charset, int chunkSize,
                long startFromRecord, long startFromChunk) {
            this.chunkNumber = (int) startFromChunk;
            this.reader = reader;
            this.charset = charset;
            this.chunkSize = chunkSize;
            advance();
            // Resume: discard the leading records already processed before the checkpoint.
            while (recordCount < startFromRecord && hasNext()) {
                recordCount++;
                advance();
            }
        }

        private void advance() {
            try {
                nextLine = reader.readLine();
                if (nextLine == null) {
                    eof = true;
                }
            } catch (IOException e) {
                nextLine = null;
                eof = true;
                log.warn("Error reading text stream at line " + recordCount, e);
            }
        }

        @Override
        public boolean hasNext() {
            return !eof && nextLine != null;
        }

        @Override
        public StreamChunk next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more lines to iterate");
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
                advance();
                linesInChunk++;

                StreamRecord record = new StreamRecord(recordCount);
                record.setEncoding(charset);
                record.setValid(true);

                if (addOutputToProperty) {
                    resultsArray.add(line);
                } else {
                    record.setContent(line.getBytes(charset));
                }
                chunk.addRecord(record);
            }

            chunk.setRecordCount(linesInChunk);
            chunk.setLastChunk(!hasNext());

            if (log.isDebugEnabled()) {
                log.debug("Text chunk with " + linesInChunk + " lines (lines "
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
