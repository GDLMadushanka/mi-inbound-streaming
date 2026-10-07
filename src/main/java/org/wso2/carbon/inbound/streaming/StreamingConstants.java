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

/**
 * Constants for the streaming ingestion feature: the inbound configuration parameter names and
 * their accepted values/defaults, plus the keys used when the parsed output is placed in a
 * message-context property.
 */
public class StreamingConstants {

    private StreamingConstants() {
    }

    // ----- Core streaming parameters -----
    public static final String STREAMING = "transport.vfs.Streaming";

    /**
     * Streaming mode, effective only when {@link #STREAMING} is true.
     * Accepted values (case-insensitive): ENTIRE_FILE (default, previous behaviour),
     * CHUNK (batch of records per message, uses ChunkIterator),
     * RECORD (one record per message, uses RowIterator).
     */
    public static final String STREAMING_MODE = "transport.vfs.StreamingMode";
    public static final String STREAMING_MODE_ENTIRE_FILE = "ENTIRE_FILE";
    public static final String STREAMING_MODE_CHUNK = "CHUNK";
    public static final String STREAMING_MODE_RECORD = "RECORD";

    /**
     * Input format of the streamed file, effective in CHUNK and RECORD modes. Determines which
     * streaming processor is used. Accepted values (case-insensitive): text, csv, json, jsonl, xml.
     */
    public static final String STREAMING_INPUT_FORMAT = "transport.vfs.StreamingInputFormat";
    public static final String STREAMING_FORMAT_TEXT = "text";
    public static final String STREAMING_FORMAT_CSV = "csv";
    public static final String STREAMING_FORMAT_JSON = "json";
    public static final String STREAMING_FORMAT_JSONL = "jsonl";
    public static final String STREAMING_FORMAT_XML = "xml";
    public static final String DEFAULT_STREAMING_INPUT_FORMAT = STREAMING_FORMAT_TEXT;

    // Canonical content types per streaming input format. In streaming CHUNK/RECORD modes the
    // content type is determined by the input format (used to pick the message builder and the
    // reader charset); the user-configured transport.vfs.ContentType does not apply.
    public static final String STREAMING_CONTENT_TYPE_TEXT = "text/plain";
    public static final String STREAMING_CONTENT_TYPE_CSV = "text/csv";
    public static final String STREAMING_CONTENT_TYPE_JSON = "application/json";
    public static final String STREAMING_CONTENT_TYPE_XML = "application/xml";

    // Buffer size (bytes) used by the streaming reader in CHUNK and RECORD modes.
    public static final String STREAMING_BUFFER_SIZE = "transport.vfs.StreamingBufferSize";
    public static final int DEFAULT_STREAMING_BUFFER_SIZE = 8192;

    // Number of records per chunk, used in CHUNK mode only.
    public static final String STREAMING_CHUNK_SIZE = "transport.vfs.StreamingChunkSize";
    public static final int DEFAULT_STREAMING_CHUNK_SIZE = 1;

    // ----- Position properties set on every streamed message -----
    //
    // Where the message sits in its file. Set on every chunk/record message in CHUNK and RECORD
    // mode, whatever the parsed output goes to - the output property or the raw message body - so
    // a sequence can always tell which rows it is holding. Values are Strings, like the other
    // Synapse properties this inbound sets, so read them with get-property().
    //
    // Record numbers are absolute and 1-based, counting data rows only: with a CSV header row the
    // first data row is record 1. They stay absolute across a checkpoint resume.
    public static final String POS_RECORD_NUMBER = "STREAMING_RECORD_NUMBER";
    public static final String POS_CHUNK_NUMBER = "STREAMING_CHUNK_NUMBER";
    public static final String POS_FIRST_RECORD = "STREAMING_FIRST_RECORD";
    public static final String POS_LAST_RECORD = "STREAMING_LAST_RECORD";
    // Valid records actually carried by this message: below the configured chunk size for the
    // final chunk, and whenever invalid records were siphoned off before injection.
    public static final String POS_CHUNK_SIZE = "STREAMING_CHUNK_SIZE";

    // Charset used to decode the streamed file in CHUNK/RECORD modes. Orthogonal to the input
    // format; defaults to UTF-8.
    public static final String STREAMING_CHARSET = "transport.vfs.StreamingCharset";
    public static final String DEFAULT_STREAMING_CHARSET = "UTF-8";

    // Name of the message-context property to hold the parsed streaming output. When set
    // (non-empty), the parsed fields are placed in this property and the message body is left
    // empty; when omitted, the raw record/chunk content is set as the message body instead.
    // MI 4.1.0 has no message variables, so the output goes to a Synapse property rather than a
    // variable - hence the parameter name. STREAMING_OUTPUT_VARIABLE is kept as a legacy alias so
    // configurations written against the variable-based inbound keep working unchanged.
    public static final String STREAMING_OUTPUT_PROPERTY = "transport.vfs.StreamingOutputProperty";

    // JSON-specific streaming parameter: the selector that marks which nodes become records.
    // Supported subset: object navigation with a single wildcard, e.g. $[*], $.store.books[*].
    public static final String STREAMING_JSON_PATH = "transport.vfs.StreamingJsonPath";
    public static final String DEFAULT_STREAMING_JSON_PATH = "$[*]";

    // CSV-specific streaming parameters.
    public static final String STREAMING_CSV_DELIMITER = "transport.vfs.StreamingCsvDelimiter";
    public static final String STREAMING_CSV_QUOTE = "transport.vfs.StreamingCsvQuote";
    public static final String STREAMING_CSV_HAS_HEADER = "transport.vfs.StreamingCsvHasHeader";
    // When true (RECORD mode raw content), the header row is prepended to each record's content.
    public static final String STREAMING_ADD_HEADERS_TO_EACH_RESULT
            = "transport.vfs.StreamingAddHeadersToEachResult";

    // Per-column JSON data types for CSV -> JSON conversion. Without it every cell is a string,
    // because CSV carries no type information. The value is the same JSON array the WSO2 CSV
    // mediation module takes for its 'dataTypes' property, e.g.
    //   [{"Column Name Or Index":"id","Is Column Name":"Yes","Data Type":"Number"},
    //    {"Column Name Or Index":"2","Is Column Name":"No","Data Type":"String"}]
    // csvDataTypes is accepted as an alias of the StreamingCsv-prefixed name used by the rest of
    // the CSV parameters.
    public static final String STREAMING_CSV_DATA_TYPES = "transport.vfs.StreamingCsvDataTypes";
    public static final String STREAMING_CSV_DATA_TYPES_ALIAS = "transport.vfs.csvDataTypes";

    // ----- Streaming error handling (per-record, in CHUNK/RECORD modes) -----
    //
    // An error action decides what happens to a record/chunk that cannot be delivered:
    //   MOVE - append it to a per-source-file sidecar in the configured folder, then continue;
    //   DROP - discard it and continue with the next record/chunk.
    // Neither fails the whole file. (A structural/IO parse error that makes the rest of the stream
    // meaningless is a separate, non-recoverable failure handled via ActionAfterFailure.)
    public static final String STREAMING_ERROR_ACTION_MOVE = "MOVE";
    public static final String STREAMING_ERROR_ACTION_DROP = "DROP";
    public static final String DEFAULT_STREAMING_ERROR_ACTION = STREAMING_ERROR_ACTION_MOVE;

    // Parse errors: a record that fails to parse (only JSONL tolerates this per-record; other
    // formats treat a parse error as a whole-file failure).
    public static final String STREAMING_PARSE_ERROR_ACTION
            = "transport.vfs.StreamingParseErrorAction";
    // Destination folder when the parse-error action is MOVE. Sidecar files insert a '.parse.fail'
    // marker before the extension (e.g. input.jsonl -> input.parse.fail.jsonl). When empty, falls
    // back to the MoveAfterFailure fault folder; if that is also empty, parse errors are logged only.
    public static final String STREAMING_PARSE_ERROR_FOLDER
            = "transport.vfs.StreamingParseErrorFolder";

    // Mediation errors: a record/chunk whose injection to the sequence failed (the data parsed
    // fine, the downstream sequence failed). Applies to every streaming format.
    public static final String STREAMING_MEDIATION_ERROR_ACTION
            = "transport.vfs.StreamingMediationErrorAction";
    // Destination folder when the mediation-error action is MOVE. Sidecar files insert a
    // '.mediation.fail' marker before the extension (e.g. input.jsonl -> input.mediation.fail.jsonl).
    // When empty, falls back to the parse-error folder, then the MoveAfterFailure fault folder; if
    // all are empty, mediation errors are logged only.
    public static final String STREAMING_MEDIATION_ERROR_FOLDER
            = "transport.vfs.StreamingMediationErrorFolder";


    // ----- Checkpointing (resumable streaming) -----
    //
    // A checkpoint is a small JSON resource written to the MI registry that records how far a file
    // has been processed, so a restart after a kill/shutdown resumes near where it stopped instead
    // of reprocessing a multi-GB file from the beginning. On resume the processor skips the already
    // consumed records; the file is identified by a cheap fingerprint (size + last-modified +
    // CRC32 of the leading bytes) plus a config hash, so a changed file or a changed configuration
    // starts fresh rather than skipping the wrong records.

    // Whether checkpointing is enabled. On by default.
    public static final String STREAMING_CHECKPOINT_ENABLED = "transport.vfs.StreamingCheckpointEnabled";
    public static final boolean DEFAULT_STREAMING_CHECKPOINT_ENABLED = true;

    // Flush the checkpoint every N confirmed units - records in RECORD mode, chunks in CHUNK mode.
    public static final String STREAMING_CHECKPOINT_INTERVAL = "transport.vfs.StreamingCheckpointInterval";
    public static final int DEFAULT_STREAMING_CHECKPOINT_INTERVAL = 1000;

    // Registry (governance) root under which per-inbound checkpoint files are stored:
    //   gov:/fileStreamingCheckpoints/{inboundName}/<fileKey>.json
    public static final String CHECKPOINT_REGISTRY_ROOT = "gov:/fileStreamingCheckpoints";

    // Number of leading file bytes hashed into the fingerprint (fixed, not user-configurable).
    public static final int CHECKPOINT_FINGERPRINT_BYTES = 65536;

    // Checkpoint schema version and fingerprint hash algorithm.
    public static final int CHECKPOINT_SCHEMA_VERSION = 1;
    public static final String CHECKPOINT_HASH_ALGORITHM = "CRC32";

    // Algorithm for the stored file identity. Distinct from the fingerprint's CRC32: that one is a
    // cheap change-detector over file bytes, this one stands in for the URI and must be one-way.
    public static final String CHECKPOINT_URI_HASH_ALGORITHM = "SHA-256";

    // ----- File-completion callbacks -----
    //
    // Optional sequences invoked once per file, after the whole file has been streamed, carrying
    // the file's totals. Separate from the per-record injecting sequence: these fire once, not per
    // record, and a failure in them never changes the outcome already decided for the file.
    public static final String STREAMING_FILE_COMPLETE_SEQUENCE
            = "transport.vfs.StreamingFileCompleteSequence";
    public static final String STREAMING_FILE_FAILURE_SEQUENCE
            = "transport.vfs.StreamingFileFailureSequence";

    // Properties set on the callback message, alongside a JSON summary body.
    public static final String CB_FILE_NAME = "STREAMING_FILE_NAME";
    public static final String CB_FILE_URI = "STREAMING_FILE_URI";
    public static final String CB_STATUS = "STREAMING_STATUS";
    public static final String CB_PROCESSED = "STREAMING_PROCESSED";
    public static final String CB_PARSE_FAILED = "STREAMING_PARSE_FAILED";
    public static final String CB_MEDIATION_FAILED = "STREAMING_MEDIATION_FAILED";
    public static final String CB_STATUS_COMPLETED = "COMPLETED";
    public static final String CB_STATUS_FAILED = "FAILED";
}
