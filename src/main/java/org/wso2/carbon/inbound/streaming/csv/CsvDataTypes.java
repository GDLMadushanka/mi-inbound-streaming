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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Column data types for CSV → JSON conversion.
 * <p>
 * Without this, every CSV cell becomes a JSON string, because CSV itself carries no type
 * information. This class reads the declared type of selected columns and emits the matching JSON
 * primitive instead.
 * <p>
 * The configuration is the same JSON shape the WSO2 CSV mediation module uses for its
 * {@code dataTypes} property, so a mapping can be moved between the two without rewriting:
 *
 * <pre>
 * [{"Column Name Or Index":"id","Is Column Name":"Yes","Data Type":"Number"},
 *  {"Column Name Or Index":"2","Is Column Name":"No","Data Type":"String"}]
 * </pre>
 *
 * <ul>
 *     <li><b>Column Name Or Index</b> - a header name, or a <b>1-based</b> column position
 *     ({@code "2"} is the second column).</li>
 *     <li><b>Is Column Name</b> - {@code Yes} (default) if the entry above is a header name,
 *     {@code No} if it is a position.</li>
 *     <li><b>Data Type</b> - one of {@code String}, {@code Boolean}, {@code Integer},
 *     {@code Number}; the match is case-insensitive.</li>
 * </ul>
 *
 * Rules are resolved against the header row once per file, not per record. Columns with no rule
 * stay strings, which is the behaviour of the inbound when this parameter is absent.
 * <p>
 * Nothing here can fail a file. A cell that does not parse as its declared type is emitted as a
 * string and warned about <em>once per column per file</em> - a streamed file can hold millions of
 * rows, so a per-cell log would drown the server log for one bad column.
 */
public final class CsvDataTypes {

    private static final Log log = LogFactory.getLog(CsvDataTypes.class);

    /** Empty mapping: every column stays a string. */
    public static final CsvDataTypes NONE = new CsvDataTypes(Collections.<Rule>emptyList());

    private static final String KEY_COLUMN = "Column Name Or Index";
    private static final String KEY_IS_COLUMN_NAME = "Is Column Name";
    private static final String KEY_DATA_TYPE = "Data Type";

    /** JSON types a column can be declared as. */
    public enum DataType { STRING, BOOLEAN, INTEGER, NUMBER }

    private final List<Rule> rules;

    private CsvDataTypes(List<Rule> rules) {
        this.rules = rules;
    }

    /**
     * Parse the configured mapping.
     * <p>
     * Configuration problems are reported here - once, when the inbound is initialised - and never
     * again while files are being streamed. A malformed document yields {@link #NONE}; a single
     * malformed rule is skipped and the rest are kept. Either way the inbound still starts, with
     * the affected columns left as strings.
     *
     * @param json the raw parameter value, may be null or blank
     * @return the parsed mapping, never null
     */
    public static CsvDataTypes parse(String json) {
        if (StringUtils.isBlank(json)) {
            return NONE;
        }

        JsonArray array;
        try {
            JsonElement root = new JsonParser().parse(json);
            if (!root.isJsonArray()) {
                log.error("CSV data types must be a JSON array of column rules, but a "
                    + describe(root) + " was configured. No column typing will be applied.");
                return NONE;
            }
            array = root.getAsJsonArray();
        } catch (RuntimeException e) {
            log.error("CSV data types could not be parsed as JSON. No column typing will be "
                + "applied. Value: " + json, e);
            return NONE;
        }

        List<Rule> parsed = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            Rule rule = parseRule(array.get(i), i);
            if (rule != null) {
                parsed.add(rule);
            }
        }
        if (parsed.isEmpty()) {
            return NONE;
        }
        return new CsvDataTypes(Collections.unmodifiableList(parsed));
    }

    private static Rule parseRule(JsonElement element, int position) {
        if (element == null || !element.isJsonObject()) {
            log.error("CSV data types: entry " + position + " is not a JSON object; skipping it.");
            return null;
        }
        JsonObject object = element.getAsJsonObject();

        String column = asString(object, KEY_COLUMN);
        if (StringUtils.isBlank(column)) {
            log.error("CSV data types: entry " + position + " has no '" + KEY_COLUMN
                + "'; skipping it.");
            return null;
        }

        String typeName = asString(object, KEY_DATA_TYPE);
        DataType type = toDataType(typeName);
        if (type == null) {
            log.error("CSV data types: entry " + position + " for column '" + column
                + "' declares an unsupported '" + KEY_DATA_TYPE + "' of '" + typeName
                + "'. Supported types are String, Boolean, Integer and Number. Skipping it.");
            return null;
        }

        // 'Is Column Name' defaults to Yes, matching the CSV mediation module.
        String isColumnName = asString(object, KEY_IS_COLUMN_NAME);
        boolean byName = StringUtils.isBlank(isColumnName) || "yes".equalsIgnoreCase(isColumnName.trim());

        if (byName) {
            return new Rule(column.trim(), -1, type);
        }

        // The configured position is 1-based, as in the CSV mediation module.
        int ordinal;
        try {
            ordinal = Integer.parseInt(column.trim());
        } catch (NumberFormatException e) {
            log.error("CSV data types: entry " + position + " is marked as a column index ('"
                + KEY_IS_COLUMN_NAME + "':'No') but '" + column + "' is not a number. Skipping it.");
            return null;
        }
        if (ordinal < 1) {
            log.error("CSV data types: entry " + position + " has column index " + ordinal
                + "; indexes are 1-based, so the first column is 1. Skipping it.");
            return null;
        }
        return new Rule(null, ordinal - 1, type);
    }

    private static String asString(JsonObject object, String key) {
        JsonElement value = object.get(key);
        if (value == null || value.isJsonNull() || !value.isJsonPrimitive()) {
            return null;
        }
        return value.getAsString();
    }

    private static DataType toDataType(String name) {
        if (StringUtils.isBlank(name)) {
            return null;
        }
        try {
            return DataType.valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String describe(JsonElement element) {
        if (element.isJsonObject()) {
            return "JSON object";
        }
        if (element.isJsonNull()) {
            return "JSON null";
        }
        return "JSON value";
    }

    /** @return true when no column has a declared type, so conversion can be skipped entirely. */
    public boolean isEmpty() {
        return rules.isEmpty();
    }

    /**
     * Resolve the configured rules against one file's header row.
     * <p>
     * Call once per file and reuse the result for every record: name lookup and index arithmetic
     * do not depend on the row.
     *
     * @param headers the parsed header names, or null/empty when the file has no header row
     * @return a converter for this file's columns, never null
     */
    public Converter resolve(String[] headers) {
        if (rules.isEmpty()) {
            return Converter.PASS_THROUGH;
        }

        Map<Integer, DataType> byIndex = new HashMap<>();
        for (Rule rule : rules) {
            int index = rule.index;
            if (rule.columnName != null) {
                index = indexOf(headers, rule.columnName);
                if (index < 0) {
                    if (headers == null || headers.length == 0) {
                        log.warn("CSV data types: column '" + rule.columnName + "' is declared by "
                            + "name, but this inbound is configured without a CSV header row "
                            + "(transport.vfs.StreamingCsvHasHeader=false). Use a 1-based column "
                            + "index with 'Is Column Name':'No' instead. Ignoring the rule.");
                    } else {
                        log.warn("CSV data types: column '" + rule.columnName + "' is not in the "
                            + "header row of this file; ignoring the rule.");
                    }
                    continue;
                }
            } else if (headers != null && headers.length > 0 && index >= headers.length) {
                log.warn("CSV data types: column index " + (index + 1) + " is past the last column"
                    + " (" + headers.length + ") of this file; ignoring the rule.");
                continue;
            }
            DataType previous = byIndex.put(index, rule.type);
            if (previous != null && previous != rule.type) {
                log.warn("CSV data types: column " + (index + 1) + " has more than one rule; "
                    + "using " + rule.type + " and ignoring " + previous + ".");
            }
        }

        if (byIndex.isEmpty()) {
            return Converter.PASS_THROUGH;
        }
        return new Converter(byIndex);
    }

    private static int indexOf(String[] headers, String columnName) {
        if (headers == null) {
            return -1;
        }
        for (int i = 0; i < headers.length; i++) {
            if (columnName.equalsIgnoreCase(headers[i])) {
                return i;
            }
        }
        return -1;
    }

    /** One configured column rule: either by header name or by 0-based column index. */
    private static final class Rule {
        private final String columnName;
        private final int index;
        private final DataType type;

        Rule(String columnName, int index, DataType type) {
            this.columnName = columnName;
            this.index = index;
            this.type = type;
        }
    }

    /**
     * Per-file view of the mapping: knows which column index carries which type, and converts a
     * raw cell into the JSON primitive to emit. Not thread-safe; one instance belongs to one
     * file's iterator, which is single-threaded.
     */
    public static final class Converter {

        /** Converter for a file with no typed columns: every cell stays a string. */
        static final Converter PASS_THROUGH = new Converter(Collections.<Integer, DataType>emptyMap());

        private final Map<Integer, DataType> typesByIndex;
        // Columns already warned about, so one unconvertible column cannot flood the log with one
        // line per row of a large file. Concurrent: with StreamingParallelism above 1 a file's
        // chunks are converted on several workers at once.
        private final Set<Integer> warned = Collections.newSetFromMap(new ConcurrentHashMap<>());

        private Converter(Map<Integer, DataType> typesByIndex) {
            this.typesByIndex = typesByIndex;
        }

        /** @return true when no column is typed, so callers can take their original fast path. */
        public boolean isPassThrough() {
            return typesByIndex.isEmpty();
        }

        /**
         * Convert one cell to the JSON primitive for its column.
         * <p>
         * A column with no declared type, an empty cell, or a value that does not parse as the
         * declared type all yield the raw string - the conversion never drops or nulls a value.
         *
         * @param columnIndex 0-based position of the cell in its row
         * @param value       the raw cell text
         * @return the JSON primitive to put in the output
         */
        public JsonPrimitive convert(int columnIndex, String value) {
            Object converted = convertValue(columnIndex, value);
            if (converted instanceof Number) {
                return new JsonPrimitive((Number) converted);
            }
            if (converted instanceof Boolean) {
                return new JsonPrimitive((Boolean) converted);
            }
            return new JsonPrimitive((String) converted);
        }

        /**
         * {@link #convert} without the JsonPrimitive wrapper, for writers that stream the JSON out
         * directly. Thread-safe.
         *
         * @return a String, a Long, a BigDecimal or a Boolean
         */
        public Object convertValue(int columnIndex, String value) {
            DataType type = typesByIndex.get(columnIndex);
            if (type == null || type == DataType.STRING || value == null) {
                return value == null ? "" : value;
            }
            // An empty cell carries no value to convert; keep the inbound's existing output for
            // blanks rather than inventing a zero or a null.
            if (value.trim().isEmpty()) {
                return value;
            }

            String text = value.trim();
            try {
                switch (type) {
                    case INTEGER:
                        // parseLong, not parseInt: CSV ids and account numbers routinely exceed
                        // the 32-bit range, and JSON has no int/long distinction anyway.
                        return Long.valueOf(Long.parseLong(text));
                    case NUMBER:
                        // BigDecimal, not double: it keeps the value and the scale exactly as
                        // written, so a monetary "120.50" stays 120.50 rather than becoming 120.5.
                        return new BigDecimal(text);
                    case BOOLEAN:
                        // Not Boolean.parseBoolean: that silently turns every unrecognised value
                        // into false, which would quietly corrupt a column holding y/n or 1/0.
                        if ("true".equalsIgnoreCase(text)) {
                            return Boolean.TRUE;
                        }
                        if ("false".equalsIgnoreCase(text)) {
                            return Boolean.FALSE;
                        }
                        throw new NumberFormatException("not a boolean: " + text);
                    default:
                        return value;
                }
            } catch (NumberFormatException | ArithmeticException e) {
                warnOnce(columnIndex, type, value);
                return value;
            }
        }

        private void warnOnce(int columnIndex, DataType type, String value) {
            if (warned.add(columnIndex)) {
                log.warn("CSV data types: column " + (columnIndex + 1) + " is declared as " + type
                    + " but holds '" + value + "', which cannot be converted. This cell and any "
                    + "further unconvertible cell in this column are emitted as strings. This is "
                    + "reported once per column per file.");
            }
        }
    }
}
