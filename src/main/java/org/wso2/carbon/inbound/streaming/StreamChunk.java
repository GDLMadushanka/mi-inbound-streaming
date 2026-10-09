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

import com.google.gson.JsonElement;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StreamChunk {

    /**
     * A chunk payload that is built on demand rather than by the reading thread. With
     * {@code StreamingParallelism} above 1 that demand comes from a worker, so per-row conversion
     * runs in parallel instead of on the single thread that reads the file.
     * <p>
     * Both methods must produce the same JSON, and must be safe to call from any thread.
     */
    public interface DeferredPayload {

        /** Build the payload as a Gson tree. */
        JsonElement buildTree();

        /** Write the payload straight to {@code out}, as {@code buildTree().toString()} would. */
        void writeTo(JsonWriter out) throws IOException;
    }

    // Multiple rows representation (for chunk mode with chunkSize > 1)
    private java.util.List<StreamRecord> records;
    private long firstRecordNumber;
    private long lastRecordNumber;
    private int recordCount;
    private int chunkNumber;

    // Metadata
    private JsonElement JSONPayload;
    // Set instead of JSONPayload when the payload is built on demand (see DeferredPayload).
    private DeferredPayload deferredPayload;
    private boolean isLastChunk;
    private boolean isValid = true;
    private String parseError;
    private Charset encoding = StandardCharsets.UTF_8;

    // Chunk configuration
    private final int chunkSize;  // No of records per chunk;

    public StreamChunk(int chunkSize) {
        this.records = new ArrayList<>();
        this.chunkSize = Math.max(1, chunkSize);
    }

    public int getChunkNumber() {
        return chunkNumber;
    }

    public void setChunkNumber(int chunkNumber) {
        this.chunkNumber = chunkNumber;
    }

    /**
     * The payload as a Gson tree. A deferred payload is built (and kept) on first call; callers
     * that only need the JSON text should use {@link #getPayloadText()}, which avoids the tree.
     */
    public JsonElement getJSONPayload() {
        if (JSONPayload == null && deferredPayload != null) {
            JSONPayload = deferredPayload.buildTree();
        }
        return JSONPayload;
    }

    /**
     * The payload as JSON text, or null if there is none. A deferred payload is written directly
     * to text without building a tree first, and is not kept.
     */
    public String getPayloadText() {
        if (JSONPayload != null) {
            return JSONPayload.toString();
        }
        if (deferredPayload == null) {
            return null;
        }
        StringWriter text = new StringWriter();
        try {
            JsonWriter out = new JsonWriter(text);
            // Lenient, like JsonElement.toString(), so the two paths emit identical text.
            out.setLenient(true);
            deferredPayload.writeTo(out);
            out.flush();
        } catch (IOException e) {
            // A StringWriter does not fail; this is only reachable through a broken builder.
            throw new UncheckedIOException(e);
        }
        return text.toString();
    }

    public void setDeferredPayload(DeferredPayload payload) {
        this.deferredPayload = payload;
    }

    public boolean isLastChunk() {
        return isLastChunk;
    }

    public void setLastChunk(boolean lastChunk) {
        isLastChunk = lastChunk;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public String getParseError() {
        return parseError;
    }

    public void setParseError(String parseError) {
        this.parseError = parseError;
    }

    public Charset getEncoding() {
        return encoding;
    }

    public void setEncoding(Charset encoding) {
        this.encoding = encoding != null ? encoding : StandardCharsets.UTF_8;
    }

    public void setJSONPayload(JsonElement payload) {
        this.JSONPayload = payload;
    }

    // Multi-row chunk support
    public int getChunkSize() {
        return chunkSize;
    }

    public List<StreamRecord> getRecords() {
        return records;
    }

    public void setRecords(List<StreamRecord> records) {
        this.records = records != null ? records : new ArrayList<>();
        this.recordCount = this.records.size();
    }

    public void addRecord(StreamRecord record) {
        if (records == null) {
            records = new ArrayList<>();
        }
        records.add(record);
        recordCount = records.size();

        if (records.size() == 1) {
            firstRecordNumber = record.getRecordNumber();
        }
        lastRecordNumber = record.getRecordNumber();
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(int count) {
        this.recordCount = count;
    }

    public long getFirstRecordNumber() {
        return firstRecordNumber;
    }

    public long getLastRecordNumber() {
        return lastRecordNumber;
    }

    @Override
    public String toString() {
        return "StreamChunk{" +
                "records=" + records +
                ", firstRecordNumber=" + firstRecordNumber +
                ", lastRecordNumber=" + lastRecordNumber +
                ", recordCount=" + recordCount +
                ", metadata=" + JSONPayload +
                ", isLastChunk=" + isLastChunk +
                ", isValid=" + isValid +
                ", parseError='" + parseError + '\'' +
                ", encoding=" + encoding +
                ", chunkSize=" + chunkSize +
                '}';
    }
}
