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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.streaming.Utils;
import org.wso2.carbon.inbound.streaming.ChunkedDataProcessor;
import org.wso2.carbon.inbound.streaming.StreamChunk;
import org.wso2.carbon.inbound.streaming.StreamRecord;
import org.wso2.carbon.inbound.streaming.StreamingException;

/**
 * Streaming processor for JSON inputs, built on Jackson's pull parser so the document is never
 * fully materialized. A user-supplied JSONPath-like selector marks which nodes become records.
 * <p>
 * Supported selector subset (validated up front; anything else is rejected):
 * <ul>
 *     <li>{@code $} - the whole document, emitted once</li>
 *     <li>{@code $[*]} - each element of the top-level array</li>
 *     <li>{@code $.store.books[*]} - descend by object keys, then each array element</li>
 *     <li>{@code $['store']['books'][*]} - bracket form (keys may contain dots)</li>
 *     <li>{@code $.store.books[*].author} - trailing navigation into each matched element</li>
 *     <li>{@code $.store} - a single node, emitted once</li>
 * </ul>
 * Not supported: recursive descent ({@code ..}), filters ({@code [?(...)]}), array indexes/slices
 * ({@code [0]}, {@code [1:3]}), and more than one wildcard.
 * <p>
 * When {@code addOutputToProperty} is enabled each match is exposed under the "payload" key: the
 * matched node as a plain object per record, or a list of nodes per chunk. Otherwise the match's
 * compact JSON text becomes the record content.
 */
public class JSONStreamingProcessor extends ChunkedDataProcessor {

    private static final Log log = LogFactory.getLog(JSONStreamingProcessor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String jsonPath;
    private final boolean addOutputToProperty;

    public JSONStreamingProcessor(int bufferSize, String jsonPath, boolean addOutputToProperty) {
        super(bufferSize);
        this.jsonPath = jsonPath;
        this.addOutputToProperty = addOutputToProperty;
    }

    @Override
    public Iterator<StreamChunk> getChunkIterator(InputStream input, String contentType, int chunkSize,
        long startFromRecord, long startFromChunk) throws StreamingException {
        CompiledPath path = CompiledPath.compile(jsonPath);
        try {
            Charset charset = detectCharset(contentType);
            JsonParser parser = MAPPER.getFactory().createParser(newReader(input, charset));
            return new JsonChunkIterator(parser, charset, Math.max(1, chunkSize), path, startFromRecord,
                startFromChunk);
        } catch (IOException e) {
            throw new StreamingException("Failed to initialize JSON processor", e, 0, false);
        }
    }

    @Override
    public Iterator<StreamRecord> getRecordIterator(InputStream input, String contentType,
        long startFromRecord) throws StreamingException {
        CompiledPath path = CompiledPath.compile(jsonPath);
        try {
            Charset charset = detectCharset(contentType);
            JsonParser parser = MAPPER.getFactory().createParser(newReader(input, charset));
            return new JsonRowIterator(parser, charset, path, startFromRecord);
        } catch (IOException e) {
            throw new StreamingException("Failed to initialize JSON processor", e, 0, false);
        }
    }

    private Reader newReader(InputStream input, Charset charset) {
        return new InputStreamReader(input, charset);
    }

    /**
     * A JSON chunk body is a JSON array of the matched nodes (each record's content is a compact
     * JSON value), not newline-joined text.
     */
    @Override
    public byte[] buildChunkBody(StreamChunk chunk) {
        return buildJsonArrayChunkBody(chunk);
    }

    /**
     * Pull cursor that walks the Jackson token stream and yields one matched node at a time.
     */
    private static final class MatchCursor {

        private final JsonParser parser;
        private final CompiledPath path;
        private boolean started = false;

        private boolean done = false;

        MatchCursor(JsonParser parser, CompiledPath path) {
            this.parser = parser;
            this.path = path;
        }

        /** @return the next matched node, or null when exhausted. */
        JsonNode nextMatch() throws IOException {
            if (done) {
                return null;
            }
            if (!started) {
                started = true;
                if (!descendToPre()) {
                    done = true;
                    return null;
                }
                if (path.wildcard) {
                    if (parser.currentToken() != JsonToken.START_ARRAY) {
                        // Selector ends in [*] but the target node is not an array.
                        done = true;
                        return null;
                    }
                } else {
                    // Single-node selector: emit the addressed node once.
                    JsonNode node = parser.readValueAsTree();
                    done = true;
                    return node;
                }
            }
            // Array mode: read the next element, applying any trailing navigation.
            while (true) {
                JsonToken t = parser.nextToken();
                if (t == JsonToken.END_ARRAY || t == null) {
                    done = true;
                    return null;
                }
                JsonNode element = parser.readValueAsTree();
                JsonNode resolved = applyPost(element);
                if (resolved != null && !resolved.isMissingNode()) {
                    return resolved;
                }
                // Trailing path absent on this element - skip it and continue.
            }
        }

        /** Descend through the pre-wildcard object keys. Leaves the parser on the target node. */
        private boolean descendToPre() throws IOException {
            JsonToken t = parser.nextToken();  // root token
            if (t == null) {
                return false;
            }
            for (String key : path.preKeys) {
                if (parser.currentToken() != JsonToken.START_OBJECT) {
                    return false;  // cannot navigate a key on a non-object
                }
                if (!findField(key)) {
                    return false;  // key absent -> no matches
                }
            }
            return true;
        }

        /** With the parser on START_OBJECT, position it on the value of {@code key}. */
        private boolean findField(String key) throws IOException {
            while (true) {
                JsonToken t = parser.nextToken();
                if (t == JsonToken.END_OBJECT || t == null) {
                    return false;
                }
                String name = parser.currentName();
                parser.nextToken();  // move to the value's first token
                if (key.equals(name)) {
                    return true;
                }
                parser.skipChildren();  // skip non-matching value
            }
        }

        /** Navigate trailing keys within a matched element (in-memory). */
        private JsonNode applyPost(JsonNode node) {
            JsonNode current = node;
            for (String key : path.postKeys) {
                if (current == null || !current.isObject()) {
                    return null;
                }
                current = current.get(key);
            }
            return current;
        }
    }

    /**
     * Record mode: one StreamRecord per matched node.
     */
    private final class JsonRowIterator implements Iterator<StreamRecord> {

        private final MatchCursor cursor;
        private final Charset charset;
        private long recordCount = 0;
        private JsonNode nextNode;

        JsonRowIterator(JsonParser parser, Charset charset, CompiledPath path,
                long startFromRecord) {
            this.cursor = new MatchCursor(parser, path);
            this.charset = charset;
            advance();
            // Resume: re-run the selector and discard the leading matches already processed.
            while (recordCount < startFromRecord && nextNode != null) {
                recordCount++;
                advance();
            }
        }

        private void advance() {
            try {
                nextNode = cursor.nextMatch();
            } catch (IOException e) {
                // The document could not be parsed any further (e.g. malformed JSON). The rest of
                // the stream is meaningless, so surface a non-recoverable failure and let the caller
                // apply the configured action-after-failure (move / delete) to the whole file.
                nextNode = null;
                throw new StreamingException(
                        "Failed to parse JSON stream at match " + recordCount, e, recordCount, false,
                        StreamingException.Kind.PARSE);
            }
        }

        @Override
        public boolean hasNext() {
            return nextNode != null;
        }

        @Override
        public StreamRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more JSON matches to iterate");
            }
            recordCount++;
            JsonNode node = nextNode;
            advance();

            StreamRecord record = new StreamRecord(recordCount);
            record.setEncoding(charset);
            record.setValid(true);

            if (addOutputToProperty) {
                // {"id":1,"name":"John"} or a scalar, as a plain Java value.
                record.setJSONPayload(Utils.convertJacksonToGson(node));
            } else {
                record.setContent(node.toString().getBytes(charset));
            }
            record.setRecordNumber(recordCount);
            return record;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }

    /**
     * Chunk mode: up to chunkSize matched nodes per StreamChunk.
     */
    private final class JsonChunkIterator implements Iterator<StreamChunk> {

        private final MatchCursor cursor;
        private final Charset charset;
        private final int chunkSize;
        private int chunkNumber = 0;
        private long recordCount = 0;
        private JsonNode nextNode;

        JsonChunkIterator(JsonParser parser, Charset charset, int chunkSize, CompiledPath path,
                long startFromRecord, long startFromChunk) {
            this.chunkNumber = (int) startFromChunk;
            this.cursor = new MatchCursor(parser, path);
            this.charset = charset;
            this.chunkSize = chunkSize;
            advance();
            // Resume: re-run the selector and discard the leading matches already processed.
            while (recordCount < startFromRecord && nextNode != null) {
                recordCount++;
                advance();
            }
        }

        private void advance() {
            try {
                nextNode = cursor.nextMatch();
            } catch (IOException e) {
                // The document could not be parsed any further (e.g. malformed JSON). The rest of
                // the stream is meaningless, so surface a non-recoverable failure and let the caller
                // apply the configured action-after-failure (move / delete) to the whole file.
                nextNode = null;
                throw new StreamingException(
                        "Failed to parse JSON stream at match " + recordCount, e, recordCount, false,
                        StreamingException.Kind.PARSE);
            }
        }

        @Override
        public boolean hasNext() {
            return nextNode != null;
        }

        @Override
        public StreamChunk next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more JSON matches to iterate");
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

            int matchesInChunk = 0;
            while (matchesInChunk < chunkSize && hasNext()) {
                recordCount++;
                JsonNode node = nextNode;
                advance();
                matchesInChunk++;

                StreamRecord record = new StreamRecord(recordCount);
                record.setEncoding(charset);
                record.setValid(true);

                if (addOutputToProperty) {
                    resultsArray.add(Utils.convertJacksonToGson(node));
                } else {
                    record.setContent(node.toString().getBytes(charset));
                }
                chunk.addRecord(record);
            }

            chunk.setRecordCount(matchesInChunk);
            chunk.setLastChunk(!hasNext());

            if (log.isDebugEnabled()) {
                log.debug("JSON chunk with " + matchesInChunk + " matches (matches "
                    + chunk.getFirstRecordNumber() + "-" + chunk.getLastRecordNumber() + ")");
            }
            return chunk;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }

    /**
     * A compiled selector: object keys before the wildcard, whether a wildcard is present, and
     * object keys to navigate within each matched element. Rejects anything outside the subset.
     */
    static final class CompiledPath {

        final String[] preKeys;
        final boolean wildcard;
        final String[] postKeys;

        private CompiledPath(List<String> preKeys, boolean wildcard, List<String> postKeys) {
            this.preKeys = preKeys.toArray(new String[0]);
            this.wildcard = wildcard;
            this.postKeys = postKeys.toArray(new String[0]);
        }

        static CompiledPath compile(String path) throws StreamingException {
            if (path == null || path.trim().isEmpty()) {
                throw invalid(path, "selector must not be empty and must start with '$'");
            }
            String p = path.trim();
            if (p.charAt(0) != '$') {
                throw invalid(path, "selector must start with '$'");
            }

            List<String> pre = new ArrayList<>();
            List<String> post = new ArrayList<>();
            boolean wildcard = false;

            int i = 1;
            int len = p.length();
            while (i < len) {
                char c = p.charAt(i);
                if (c == '.') {
                    i++;
                    if (i < len && p.charAt(i) == '.') {
                        throw invalid(path, "recursive descent '..' is not supported");
                    }
                    int start = i;
                    while (i < len && p.charAt(i) != '.' && p.charAt(i) != '[') {
                        i++;
                    }
                    String key = p.substring(start, i);
                    if (key.isEmpty()) {
                        throw invalid(path, "empty object key after '.'");
                    }
                    if (key.equals("*")) {
                        throw invalid(path, "object wildcard '.*' is not supported");
                    }
                    (wildcard ? post : pre).add(key);
                } else if (c == '[') {
                    int close = p.indexOf(']', i);
                    if (close < 0) {
                        throw invalid(path, "unclosed '['");
                    }
                    String inner = p.substring(i + 1, close).trim();
                    if (inner.equals("*")) {
                        if (wildcard) {
                            throw invalid(path, "only a single wildcard '[*]' is supported");
                        }
                        wildcard = true;
                    } else if (inner.length() >= 2
                            && (inner.charAt(0) == '\'' || inner.charAt(0) == '"')
                            && inner.charAt(inner.length() - 1) == inner.charAt(0)) {
                        String key = inner.substring(1, inner.length() - 1);
                        (wildcard ? post : pre).add(key);
                    } else {
                        throw invalid(path, "unsupported selector '[" + inner
                            + "]' - array index, slice and filter selectors are not supported");
                    }
                    i = close + 1;
                } else {
                    throw invalid(path, "unexpected character '" + c + "' at position " + i);
                }
            }
            return new CompiledPath(pre, wildcard, post);
        }

        private static StreamingException invalid(String path, String reason) {
            return new StreamingException("Invalid StreamingJsonPath '" + path + "': " + reason,
                null, 0, false);
        }
    }
}
