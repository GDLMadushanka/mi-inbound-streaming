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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.List;
import org.wso2.carbon.inbound.streaming.StreamChunk;

/**
 * The JSON payload of one CSV chunk in property-output mode, kept as the raw cells the reader
 * tokenized. Type conversion and JSON building happen only when the payload is asked for - on a
 * worker thread when the file is mediated in parallel - instead of on the thread reading the file.
 * <p>
 * Produces exactly what the reader used to build eagerly: an array with one element per row, each
 * an object keyed by the header names (when headers are added to each result) or an array of cell
 * values.
 */
final class CsvChunkPayload implements StreamChunk.DeferredPayload {

    private static final Gson GSON = new Gson();

    private final String[] headers;
    private final List<String[]> rows;
    private final CsvDataTypes.Converter converter;
    private final boolean asObjects;
    // A JsonObject keeps the last value of a repeated key in the first key's position; streaming
    // the cells out would emit the key twice instead. Such files take the tree path.
    private final boolean writeDirectly;

    /**
     * @param headers           the file's header names, or null
     * @param rows              each row's cells, as tokenized (not copied)
     * @param converter         the file's column type converter (thread-safe)
     * @param asObjects         true to key each row's cells by header name
     * @param duplicateHeaders  true if the header row repeats a name
     */
    CsvChunkPayload(String[] headers, List<String[]> rows, CsvDataTypes.Converter converter,
                    boolean asObjects, boolean duplicateHeaders) {
        this.headers = headers;
        this.rows = rows;
        this.converter = converter;
        this.asObjects = asObjects;
        this.writeDirectly = !(asObjects && duplicateHeaders);
    }

    @Override
    public JsonElement buildTree() {
        JsonArray results = new JsonArray();
        for (String[] row : rows) {
            if (asObjects) {
                JsonObject object = new JsonObject();
                for (int i = 0; i < headers.length && i < row.length; i++) {
                    object.add(headers[i], converter.convert(i, row[i]));
                }
                results.add(object);
            } else {
                JsonArray values = new JsonArray();
                for (int i = 0; i < row.length; i++) {
                    values.add(converter.convert(i, row[i]));
                }
                results.add(values);
            }
        }
        return results;
    }

    @Override
    public void writeTo(JsonWriter out) throws IOException {
        if (!writeDirectly) {
            GSON.toJson(buildTree(), out);
            return;
        }
        out.beginArray();
        for (String[] row : rows) {
            if (asObjects) {
                out.beginObject();
                for (int i = 0; i < headers.length && i < row.length; i++) {
                    out.name(headers[i]);
                    writeValue(out, converter.convertValue(i, row[i]));
                }
                out.endObject();
            } else {
                out.beginArray();
                for (int i = 0; i < row.length; i++) {
                    writeValue(out, converter.convertValue(i, row[i]));
                }
                out.endArray();
            }
        }
        out.endArray();
    }

    /** Write one converted cell the way Gson writes the equivalent JsonPrimitive. */
    private static void writeValue(JsonWriter out, Object value) throws IOException {
        if (value instanceof Number) {
            out.value((Number) value);
        } else if (value instanceof Boolean) {
            out.value(((Boolean) value).booleanValue());
        } else {
            out.value((String) value);
        }
    }
}
