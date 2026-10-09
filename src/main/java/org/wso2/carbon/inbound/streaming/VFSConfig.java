/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
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

import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.crypto.CryptoUtil;
import org.wso2.carbon.inbound.streaming.csv.CsvDataTypes;
import org.wso2.carbon.inbound.streaming.StreamingConstants;
import org.apache.commons.vfs2.FileSystemException;

import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.vfs2.provider.UriParser.extractQueryParams;

/**
 * Holds information about an entry in the VFS transport poll table used by the
 * VFS Transport Listener
 */
public class VFSConfig {

    // operation after scan
    public static final int DELETE = 0;
    public static final int MOVE = 1;
    public static final int NONE = 2;
    private static final long FILE_SIZE_CHECK_INTERVAL_DEFAULT = 1000L;
    private static final Log log = LogFactory.getLog(VFSConfig.class);
    /**
     * File or Directory to scan
     */
    private String fileURI;
    /**
     * The URI to send replies to. May be null.
     */
    private String replyFileURI;
    private String replayFileName;
    /**
     * file name pattern for a directory or compressed file entry
     */
    private String fileNamePattern;
    /**
     * Content-Type to use for the message
     */
    private String contentType;
    /**
     * Should the last updated time timestamp be updated
     */
    private boolean updateLastModified;
    /**
     * action to take after a successful poll
     */
    private int actionAfterProcess = DELETE;
    /**
     * action to take after a poll with errors
     */
    private int actionAfterErrors = DELETE;
    /**
     * action to take after a failed poll
     */
    private int actionAfterFailure = DELETE;
    /**
     * where to move the file after processing
     */
    private String moveAfterProcess;
    /**
     * where to move the file after encountering some errors
     */
    private String moveAfterErrors;
    /**
     * where to move the file after total failure
     */
    private String moveAfterFailure;
    /**
     * moved file will have this formatted timestamp prefix
     */
    private DateFormat moveTimestampFormat;
    /**
     * containing the time in [ms] between the size check on files (to avoid reading files which are currently written)
     */
    private long checkSizeInterval;
    /**
     * does the checkSize Lock mechanisme take empty files or not, default = false
     */
    private boolean checkSizeIgnoreEmpty;
    private boolean streaming;
    private String streamingMode;
    private String streamingInputFormat;
    private String streamingContentType;
    private String streamingCharset;
    private String streamingJsonPath;
    private int streamingBufferSize;
    private int streamingChunkSize;
    private String streamingOutputProperty;
    private boolean streamingAddHeadersToEachResult;
    private char streamingCsvDelimiter;
    private boolean streamingCheckpointEnabled;
    private int streamingCheckpointInterval;
    private int streamingParallelism;
    private String streamingFileCompleteSequence;
    private String streamingFileFailureSequence;
    private CsvDataTypes streamingCsvDataTypes;
    private String streamingCsvDataTypesRaw;
    private char streamingCsvQuote;
    private boolean streamingCsvHasHeader;
    private String streamingParseErrorAction;
    private String streamingParseErrorFolder;
    private String streamingMediationErrorAction;
    private String streamingMediationErrorFolder;
    private boolean build;
    private int maxRetryCount;
    private long reconnectTimeout;
    private boolean fileLocking;
    private CryptoUtil cryptoUtil;
    private Properties secureVaultProperties;
    /**
     * Only files smaller than this limit will get processed, it can be configured with param
     * "transport.vfs.FileSizeLimit", and this will have default value -1 which means unlimited file size
     * This should be specified in bytes
     */
    private double fileSizeLimit = VFSConstants.DEFAULT_TRANSPORT_FILE_SIZE_LIMIT;
    private String moveAfterMoveFailure;
    private int nextRetryDurationForFailedMove;
    private String failedRecordFileName;
    private String failedRecordFileDestination;
    private String failedRecordTimestampFormat;
    private Integer fileProcessingInterval;
    private Integer fileProcessingCount;
    private Map<String, String> vfsSchemeProperties;
    private boolean autoLockRelease;
    private Long autoLockReleaseInterval;
    private Boolean autoLockReleaseSameNode;
    private boolean distributedLock;
    private String fileSortParam;
    private boolean fileSortAscending;
    private boolean forceCreateFolder;
    private String subfolderTimestamp;
    private Long distributedLockTimeout;
    private volatile boolean canceled;
    private boolean clusterAware;
    /**
     * This parameter is used decide whether the resolving hostname IP of URIs are done at deployment or dynamically.
     * At usage default id 'false' which lead hostname resolution at deployment
     */
    private boolean resolveHostsDynamically = false;
    private boolean fileNotFoundLogged = false;
    private ParameterInclude params;
    private Long minimumAge = null; //defines a minimum age of a file before being consumed. Use to avoid just written files to be consumed
    private Long maximumAge = null; //defines a maximum age of a file being consumed. Old files will stay in the directory
    private boolean append;
    private boolean avoidPermissionCheck = false;
    private boolean passive;
    private Long failedRecordNextRetryDuration = 30000L;

    public VFSConfig(Properties properties) {
        // Basic URIs
        this.fileURI = properties.getProperty(VFSConstants.TRANSPORT_FILE_FILE_URI);
        this.replyFileURI = properties.getProperty(VFSConstants.REPLY_FILE_URI);
        this.replayFileName = properties.getProperty(VFSConstants.REPLY_FILE_NAME);
        this.fileNamePattern = properties.getProperty(VFSConstants.TRANSPORT_FILE_FILE_NAME_PATTERN);
        this.contentType = properties.getProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE);

        // Boolean/string flags
        this.updateLastModified = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.UPDATE_LAST_MODIFIED, "false"));
        this.streaming = Boolean.parseBoolean(
                properties.getProperty(StreamingConstants.STREAMING, "false"));

        // Streaming mode and its parameters (only relevant when streaming is enabled).
        this.streamingMode = properties.getProperty(
                StreamingConstants.STREAMING_MODE, StreamingConstants.STREAMING_MODE_ENTIRE_FILE);
        this.streamingInputFormat = properties.getProperty(
                StreamingConstants.STREAMING_INPUT_FORMAT, StreamingConstants.DEFAULT_STREAMING_INPUT_FORMAT);
        this.streamingJsonPath = properties.getProperty(
                StreamingConstants.STREAMING_JSON_PATH, StreamingConstants.DEFAULT_STREAMING_JSON_PATH);
        this.streamingBufferSize = Integer.parseInt(
                properties.getProperty(StreamingConstants.STREAMING_BUFFER_SIZE,
                        String.valueOf(StreamingConstants.DEFAULT_STREAMING_BUFFER_SIZE)));
        this.streamingChunkSize = Integer.parseInt(
                properties.getProperty(StreamingConstants.STREAMING_CHUNK_SIZE,
                        String.valueOf(StreamingConstants.DEFAULT_STREAMING_CHUNK_SIZE)));
        // Prefer the 4.1.0 name; fall back to the legacy 'StreamingOutputVariable' key.
        this.streamingOutputProperty = properties.getProperty(
                StreamingConstants.STREAMING_OUTPUT_PROPERTY, null);
        this.streamingAddHeadersToEachResult = Boolean.parseBoolean(
                properties.getProperty(StreamingConstants.STREAMING_ADD_HEADERS_TO_EACH_RESULT, "false"));
        this.streamingCsvDelimiter = firstCharOrDefault(
                properties.getProperty(StreamingConstants.STREAMING_CSV_DELIMITER), ',');
        this.streamingCsvQuote = firstCharOrDefault(
                properties.getProperty(StreamingConstants.STREAMING_CSV_QUOTE), '"');
        this.streamingCsvHasHeader = Boolean.parseBoolean(
                properties.getProperty(StreamingConstants.STREAMING_CSV_HAS_HEADER, "true"));
        this.streamingCheckpointEnabled = Boolean.parseBoolean(
                properties.getProperty(StreamingConstants.STREAMING_CHECKPOINT_ENABLED,
                        String.valueOf(StreamingConstants.DEFAULT_STREAMING_CHECKPOINT_ENABLED)));
        this.streamingCheckpointInterval = Integer.parseInt(
                properties.getProperty(StreamingConstants.STREAMING_CHECKPOINT_INTERVAL,
                        String.valueOf(StreamingConstants.DEFAULT_STREAMING_CHECKPOINT_INTERVAL)));
        this.streamingParallelism = parseStreamingParallelism(
                properties.getProperty(StreamingConstants.STREAMING_PARALLELISM));
        if (streamingCheckpointEnabled && streamingParallelism > 1
                && streamingCheckpointInterval < streamingParallelism) {
            // Not wrong, just not useful: the units in flight are replayed after a crash whatever
            // the interval, so flushing more often than that only adds registry writes.
            log.info(StreamingConstants.STREAMING_CHECKPOINT_INTERVAL + " ("
                    + streamingCheckpointInterval + ") is below " + StreamingConstants.STREAMING_PARALLELISM
                    + " (" + streamingParallelism + "). Up to "
                    + streamingParallelism * StreamingConstants.STREAMING_WINDOW_PER_WORKER
                    + " in-flight units are re-mediated after a crash regardless; a lower interval"
                    + " only adds checkpoint writes.");
        }
        this.streamingFileCompleteSequence =
                properties.getProperty(StreamingConstants.STREAMING_FILE_COMPLETE_SEQUENCE);
        this.streamingFileFailureSequence =
                properties.getProperty(StreamingConstants.STREAMING_FILE_FAILURE_SEQUENCE);
        // Parsed here, once per inbound, so a bad mapping is reported at initialisation rather
        // than on every polled file.
        this.streamingCsvDataTypesRaw = properties.getProperty(
                StreamingConstants.STREAMING_CSV_DATA_TYPES,
                properties.getProperty(StreamingConstants.STREAMING_CSV_DATA_TYPES_ALIAS));
        this.streamingCsvDataTypes = CsvDataTypes.parse(this.streamingCsvDataTypesRaw);
        this.streamingParseErrorAction = properties.getProperty(
                StreamingConstants.STREAMING_PARSE_ERROR_ACTION,
                StreamingConstants.DEFAULT_STREAMING_ERROR_ACTION);
        this.streamingParseErrorFolder =
                properties.getProperty(StreamingConstants.STREAMING_PARSE_ERROR_FOLDER);
        this.streamingMediationErrorAction = properties.getProperty(
                StreamingConstants.STREAMING_MEDIATION_ERROR_ACTION,
                StreamingConstants.DEFAULT_STREAMING_ERROR_ACTION);
        this.streamingMediationErrorFolder =
                properties.getProperty(StreamingConstants.STREAMING_MEDIATION_ERROR_FOLDER);
        this.streamingCharset = properties.getProperty(
                StreamingConstants.STREAMING_CHARSET, StreamingConstants.DEFAULT_STREAMING_CHARSET);
        // In streaming CHUNK/RECORD modes the content type is fixed by the input format. The charset
        // is a separate, user-configurable axis; fold it into the content type so the streaming
        // reader (which parses ';charset=') and the message builder both pick it up.
        String baseContentType = resolveStreamingContentType(this.streamingInputFormat);
        if (streaming && !StreamingConstants.STREAMING_MODE_ENTIRE_FILE.equalsIgnoreCase(streamingMode)) {
            validateStreamingCharset(this.streamingCharset);
            this.streamingContentType = baseContentType + "; charset=" + this.streamingCharset;
            warnOnContentTypeMismatch(baseContentType);
        } else {
            this.streamingContentType = baseContentType;
        }
        this.build = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_BUILD, "false"));
        this.fileLocking = VFSConstants.TRANSPORT_FILE_LOCKING_ENABLED.equalsIgnoreCase(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_LOCKING, VFSConstants.TRANSPORT_FILE_LOCKING_DISABLED));
        this.forceCreateFolder = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.FORCE_CREATE_FOLDER, "false"));
        this.resolveHostsDynamically = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_RESOLVEHOST_DYNAMICALLY, "false"));
        this.autoLockRelease = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_AUTO_LOCK_RELEASE, "false"));
        this.autoLockReleaseSameNode = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_AUTO_LOCK_RELEASE_SAME_NODE, "false"));
        this.distributedLock = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_DISTRIBUTED_LOCK, "false"));
        this.clusterAware = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.CLUSTER_AWARE, "false"));

        // Actions after processing
        this.actionAfterProcess = parseAction(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_ACTION_AFTER_PROCESS));
        this.actionAfterErrors = parseAction(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_ACTION_AFTER_ERRORS));
        this.actionAfterFailure = parseAction(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_ACTION_AFTER_FAILURE));

        // Move locations
        this.moveAfterProcess = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS);
        this.moveAfterErrors = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_ERRORS);
        this.moveAfterFailure = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILURE);
        this.moveAfterMoveFailure = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE);

        // Timestamp formatting
        String tsFormat = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_TIMESTAMP_FORMAT);
        if (tsFormat != null) {
            this.moveTimestampFormat = new SimpleDateFormat(tsFormat);
        }
        this.failedRecordTimestampFormat =
                properties.getProperty(VFSConstants.TRANSPORT_FAILED_RECORD_TIMESTAMP_FORMAT,
                        VFSConstants.DEFAULT_TRANSPORT_FAILED_RECORD_TIMESTAMP_FORMAT);

        // Check size mechanism
        this.checkSizeInterval = Optional.ofNullable(properties.getProperty(VFSConstants.TRANSPORT_CHECK_SIZE_INTERVAL))
                .filter(s -> !s.isEmpty())
                .map(Long::parseLong)
                .orElse(FILE_SIZE_CHECK_INTERVAL_DEFAULT);
        this.checkSizeIgnoreEmpty = Boolean.parseBoolean(properties.getProperty(VFSConstants.TRANSPORT_CHECK_SIZE_IGNORE_EMPTY, "false"));

        // Retry / reconnect
        this.maxRetryCount = Integer.parseInt(
                properties.getProperty(VFSConstants.MAX_RETRY_COUNT,
                        String.valueOf(VFSConstants.DEFAULT_MAX_RETRY_COUNT)));
        this.reconnectTimeout = Long.parseLong(
                properties.getProperty(VFSConstants.RECONNECT_TIMEOUT,
                        String.valueOf(VFSConstants.DEFAULT_RECONNECT_TIMEOUT)));
        this.nextRetryDurationForFailedMove = Integer.parseInt(
                properties.getProperty(VFSConstants.TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION,
                        String.valueOf(VFSConstants.DEFAULT_NEXT_RETRY_DURATION)));

        // Failed record handling
        this.failedRecordFileName = properties.getProperty(
                VFSConstants.TRANSPORT_FAILED_RECORDS_FILE_NAME,
                VFSConstants.DEFAULT_FAILED_RECORDS_FILE_NAME);
        this.failedRecordFileDestination = properties.getProperty(
                VFSConstants.TRANSPORT_FAILED_RECORDS_FILE_DESTINATION,
                VFSConstants.DEFAULT_FAILED_RECORDS_FILE_DESTINATION);

        // Processing interval/count
        String intervalStr = properties.getProperty(VFSConstants.TRANSPORT_FILE_INTERVAL);
        if (intervalStr != null) {
            this.fileProcessingInterval = Integer.valueOf(intervalStr);
        }
        String countStr = properties.getProperty(VFSConstants.TRANSPORT_FILE_COUNT);
        if (countStr != null) {
            this.fileProcessingCount = Integer.valueOf(countStr);
        }

        // Lock release configs
        String autoReleaseIntervalStr = properties.getProperty(VFSConstants.TRANSPORT_AUTO_LOCK_RELEASE_INTERVAL);
        if (autoReleaseIntervalStr != null) {
            this.autoLockReleaseInterval = Long.valueOf(autoReleaseIntervalStr);
        }

        String distLockTimeoutStr = properties.getProperty(VFSConstants.TRANSPORT_DISTRIBUTED_LOCK_TIMEOUT);
        if (distLockTimeoutStr != null) {
            this.distributedLockTimeout = Long.valueOf(distLockTimeoutStr);
        }

        // Sorting configs
        this.fileSortParam = properties.getProperty(VFSConstants.FILE_SORT_PARAM);
        this.fileSortAscending = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.FILE_SORT_ORDER, "true"));

        // File size limit
        this.fileSizeLimit = Double.parseDouble(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_SIZE_LIMIT,
                        String.valueOf(VFSConstants.DEFAULT_TRANSPORT_FILE_SIZE_LIMIT)));

        // Subfolder timestamp
        this.subfolderTimestamp = properties.getProperty(VFSConstants.SUBFOLDER_TIMESTAMP);

        // Min/Max Age
        String minAgeStr = properties.getProperty(VFSConstants.TRANSPORT_FILE_MINIMUM_AGE);
        if (minAgeStr != null && !minAgeStr.isEmpty()) {
            this.minimumAge = Long.valueOf(minAgeStr);
        }
        String maxAgeStr = properties.getProperty(VFSConstants.TRANSPORT_FILE_MAXIMUM_AGE);
        if (maxAgeStr != null && !maxAgeStr.isEmpty()) {
            this.maximumAge = Long.valueOf(maxAgeStr);
        }

        // Secure vault related
        this.secureVaultProperties = properties;
        this.append = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.APPEND, "false"));

        this.passive = Boolean.parseBoolean(
                properties.getProperty("vfs.passive", "false"));

        String retryDurationStr = properties.getProperty(VFSConstants.TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION);
        if (StringUtils.isNotEmpty(retryDurationStr)) {
            try {
                this.failedRecordNextRetryDuration = Long.parseLong(retryDurationStr);
            } catch (NumberFormatException e) {
                this.failedRecordNextRetryDuration = 30000L;
                log.warn("Invalid failedRecordNextRetryDuration value: " + retryDurationStr +
                        ". Using default: " + failedRecordNextRetryDuration);
            }
        }

        try {
            this.vfsSchemeProperties = extractQueryParams(this.fileURI);
        } catch (FileSystemException e) {
            this.vfsSchemeProperties = new HashMap<>();
            if (log.isDebugEnabled()) {
                log.debug("Error extracting query params from URI: " + Utils.maskURLPassword(this.fileURI), e);
            }
        }
    }

    /**
     * Small helper to map actions to int (DELETE / MOVE etc.)
     */
    private int parseAction(String action) {
        if (action == null) {
            return DELETE; // default
        }
        if ("MOVE".equalsIgnoreCase(action)) {
            return MOVE;
        } else if ("NONE".equalsIgnoreCase(action)) {
            return NONE;
        }
        return DELETE;
    }


    public String getFileURI() {
        if (resolveHostsDynamically) {
            try {
                return Utils.resolveUriHost(fileURI);
            } catch (UnknownHostException | FileSystemException e) {
                String message = "Unable to resolve the hostname of transport.vfs.FileURI : " +
                        Utils.maskURLPassword(fileURI);
                VFSTransportErrorHandler.logException(log, VFSTransportErrorHandler.LogType.WARN, message, e);
            }
        }
        return fileURI;
    }

    public boolean isClusterAware() {
        return clusterAware;
    }

    public Map<String, String> getVfsSchemeProperties() {
        return vfsSchemeProperties;
    }

    private String resolveHostAtDeployment(String uri) throws AxisFault {
        if (!resolveHostsDynamically) {
            try {
                return Utils.resolveUriHost(uri, new StringBuilder());
            } catch (FileSystemException e) {
                String errorMsg = "Unable to decode the malformed URI : " + Utils.maskURLPassword(uri);
                // log the error since if we only throw AxisFault, we won't get the entire stacktrace in logs to
                // identify root cause to users
                VFSTransportErrorHandler.handleException(log, errorMsg, e);

            } catch (UnknownHostException e) {
                String errorMsg = "Error occurred while resolving hostname of URI : " + Utils.maskURLPassword(uri);
                //log the error since if we only throw AxisFault, we won't get the entire stacktrace in logs to
                // identify root cause to users
                VFSTransportErrorHandler.handleException(log, errorMsg, e);
            }
        }
        return uri;
    }

    /**
     * Iterate ParameterInclude and decrypt parameters if required.
     *
     * @param params ParameterInclude instance
     * @throws AxisFault
     */
    private void decryptParamsIfRequired(ParameterInclude params) throws AxisFault {
        for (Parameter param : params.getParameters()) {
            if (param != null && param.getValue() != null && param.getValue() instanceof String) {
                param.setValue(decryptIfRequired(param.getValue().toString()));
            }
        }
    }

    /**
     * Helper method to decrypt parameters if required.
     * If the parameter is defined as - {wso2:vault-decrypt('Parameter')}, then this method will treat it as decryption
     * required and do the relevant decryption for that part.
     *
     * @param parameter
     * @return parameter
     * @throws AxisFault
     */
    private String decryptIfRequired(String parameter) throws AxisFault {
        if (parameter != null && !parameter.isEmpty()) {
            // Create a Pattern object
            Pattern r = Pattern.compile("\\{wso2:vault-decrypt\\('(.*?)'\\)\\}");

            // Now create matcher object.
            Matcher m = r.matcher(parameter);
            if (m.find()) {
                if (cryptoUtil == null) {
                    cryptoUtil = new CryptoUtil(secureVaultProperties);
                }
                if (!cryptoUtil.isInitialized()) {
                    VFSTransportErrorHandler.handleException(log, "Error initialising cryptoutil");
                }
                String toDecrypt = m.group(1);
                toDecrypt = new String(cryptoUtil.decrypt(toDecrypt.getBytes()));
                parameter = m.replaceFirst(toDecrypt);
            }
        }
        return parameter;
    }

    public String getReplyFileURI() {
        return replyFileURI;
    }

    public String getFileNamePattern() {
        return fileNamePattern;
    }

    public String getContentType() {
        return contentType;
    }

    public boolean isUpdateLastModified() {
        return updateLastModified;
    }

    public int getActionAfterProcess() {
        return actionAfterProcess;
    }

    public int getActionAfterErrors() {
        return actionAfterErrors;
    }

    public int getActionAfterFailure() {
        return actionAfterFailure;
    }

    public String getMoveAfterProcess() {
        return moveAfterProcess;
    }

    public String getMoveAfterErrors() {
        return moveAfterErrors;
    }

    public String getMoveAfterFailure() {
        return moveAfterFailure;
    }

    public DateFormat getMoveTimestampFormat() {
        return moveTimestampFormat;
    }

    public long getCheckSizeInterval() {
        return checkSizeInterval;
    }

    public boolean getCheckSizeIgnoreEmpty() {
        return checkSizeIgnoreEmpty;
    }

    public boolean isStreaming() {
        return streaming;
    }

    public String getStreamingMode() {
        return streamingMode;
    }

    public String getStreamingInputFormat() {
        return streamingInputFormat;
    }

    /**
     * Content type derived from the streaming input format (not user-configurable). Used in
     * CHUNK/RECORD streaming modes to pick the message builder and the reader charset.
     */
    public String getStreamingContentType() {
        return streamingContentType;
    }

    public String getStreamingCharset() {
        return streamingCharset;
    }

    public String getStreamingJsonPath() {
        return streamingJsonPath;
    }

    public int getStreamingBufferSize() {
        return streamingBufferSize;
    }

    public int getStreamingChunkSize() {
        return streamingChunkSize;
    }

    /**
     * Name of the message-context property to hold the parsed streaming output, or null/empty
     * if the output should be set as the message body instead. MI 4.1.0 has no message variables,
     * so the parsed output is carried in a Synapse property.
     */
    public String getStreamingOutputProperty() {
        return streamingOutputProperty;
    }

    /**
     * True when a streaming output property name is configured, meaning parsed fields should be
     * placed in that property and the message body left empty.
     */
    public boolean isStreamingAddOutputToProperty() {
        return streamingOutputProperty != null && !streamingOutputProperty.trim().isEmpty();
    }

    public boolean isStreamingAddHeadersToEachResult() {
        return streamingAddHeadersToEachResult;
    }

    public char getStreamingCsvDelimiter() {
        return streamingCsvDelimiter;
    }

    public char getStreamingCsvQuote() {
        return streamingCsvQuote;
    }

    public boolean isStreamingCsvHasHeader() {
        return streamingCsvHasHeader;
    }

    /**
     * True when a checkpoint should be written so a restart resumes mid-file. On by default.
     */
    public boolean isStreamingCheckpointEnabled() {
        return streamingCheckpointEnabled;
    }

    /**
     * Flush the checkpoint every this many confirmed units (records in RECORD mode, chunks in
     * CHUNK mode).
     */
    public int getStreamingCheckpointInterval() {
        return streamingCheckpointInterval;
    }

    public int getStreamingParallelism() {
        return streamingParallelism;
    }

    /** Parse transport.vfs.StreamingParallelism, falling back to serial (1) on a bad value. */
    private static int parseStreamingParallelism(String value) {
        if (value == null || value.trim().isEmpty()) {
            return StreamingConstants.DEFAULT_STREAMING_PARALLELISM;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed >= 1) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
            // reported below
        }
        log.warn("Invalid " + StreamingConstants.STREAMING_PARALLELISM + " value '" + value
                + "'; it must be a positive integer. Mediating serially (1).");
        return StreamingConstants.DEFAULT_STREAMING_PARALLELISM;
    }

    /**
     * Name of a sequence to invoke once, after a file has been streamed to completion, or null
     * when no completion callback is configured.
     */
    public String getStreamingFileCompleteSequence() {
        return streamingFileCompleteSequence;
    }

    /**
     * Name of a sequence to invoke once, after a file has been treated as a whole-file failure, or
     * null when no failure callback is configured.
     */
    public String getStreamingFileFailureSequence() {
        return streamingFileFailureSequence;
    }

    /**
     * Per-column JSON data types applied when CSV records are converted to JSON. Never null;
     * {@link CsvDataTypes#NONE} when the parameter is absent, in which case every cell stays a
     * string.
     */
    public CsvDataTypes getStreamingCsvDataTypes() {
        return streamingCsvDataTypes;
    }

    /**
     * The raw column data-types parameter as configured, for the checkpoint config hash. Changing
     * the declared types changes the parsed output, so a resume must not cross such a change.
     */
    public String getStreamingCsvDataTypesRaw() {
        return streamingCsvDataTypesRaw;
    }

    /**
     * Action for a record that fails to parse (MOVE or DROP). Applies to JSONL, whose parse errors
     * are per-record; defaults to MOVE.
     */
    public String getStreamingParseErrorAction() {
        return streamingParseErrorAction;
    }

    /**
     * VFS URI of the folder where parse-error records are moved (when the parse-error action is
     * MOVE), or null/empty if not configured (callers then fall back to the fault folder, or
     * log-only).
     */
    public String getStreamingParseErrorFolder() {
        return streamingParseErrorFolder;
    }

    /**
     * Action for a record/chunk whose mediation fails (MOVE or DROP). Applies to every streaming
     * format; defaults to MOVE.
     */
    public String getStreamingMediationErrorAction() {
        return streamingMediationErrorAction;
    }

    /**
     * VFS URI of the folder where mediation-error records are moved (when the mediation-error action
     * is MOVE), or null/empty if not configured (callers then fall back to the parse-error folder,
     * the fault folder, or log-only).
     */
    public String getStreamingMediationErrorFolder() {
        return streamingMediationErrorFolder;
    }

    /**
     * Returns the first character of the given value, or the fallback when the value is
     * null or empty. Used to parse single-character CSV parameters (delimiter, quote).
     */
    private static char firstCharOrDefault(String value, char fallback) {
        return (value != null && !value.isEmpty()) ? value.charAt(0) : fallback;
    }

    /**
     * Map a streaming input format to its canonical content type. Unknown formats fall back to
     * plain text.
     */
    private static String resolveStreamingContentType(String format) {
        if (format == null) {
            return StreamingConstants.STREAMING_CONTENT_TYPE_TEXT;
        }
        switch (format.toLowerCase()) {
            case StreamingConstants.STREAMING_FORMAT_CSV:
                return StreamingConstants.STREAMING_CONTENT_TYPE_CSV;
            case StreamingConstants.STREAMING_FORMAT_JSON:
            case StreamingConstants.STREAMING_FORMAT_JSONL:
                // Each JSONL record is itself a JSON value, so records build as application/json.
                return StreamingConstants.STREAMING_CONTENT_TYPE_JSON;
            case StreamingConstants.STREAMING_FORMAT_XML:
                return StreamingConstants.STREAMING_CONTENT_TYPE_XML;
            case StreamingConstants.STREAMING_FORMAT_TEXT:
            default:
                return StreamingConstants.STREAMING_CONTENT_TYPE_TEXT;
        }
    }

    /**
     * Warn at startup if the user configured a Content-Type whose media type differs from the one
     * implied by the streaming input format (which takes precedence and will be used instead).
     *
     * @param baseContentType the format-derived media type, without any charset parameter
     */
    private void warnOnContentTypeMismatch(String baseContentType) {
        if (contentType == null || contentType.trim().isEmpty()) {
            return;
        }
        int semi = contentType.indexOf(';');
        String userMediaType = (semi > 0 ? contentType.substring(0, semi) : contentType).trim();
        if (!userMediaType.equalsIgnoreCase(baseContentType)) {
            log.warn("Configured Content-Type '" + contentType + "' is ignored while streaming "
                    + "input format '" + streamingInputFormat + "'. Using '" + baseContentType
                    + "' instead.");
        }
    }

    /**
     * Validate the configured streaming charset, failing fast with a clear message on an unknown
     * or malformed charset name.
     */
    private static void validateStreamingCharset(String name) {
        boolean supported;
        try {
            supported = Charset.isSupported(name);
        } catch (IllegalArgumentException e) {
            supported = false;  // malformed charset name
        }
        if (!supported) {
            throw new IllegalArgumentException("Unsupported " + StreamingConstants.STREAMING_CHARSET
                    + " value: '" + name + "'");
        }
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public long getReconnectTimeout() {
        return reconnectTimeout;
    }

    public boolean isFileLocking() {
        return fileLocking;
    }

    public CryptoUtil getCryptoUtil() {
        return cryptoUtil;
    }

    public Properties getSecureVaultProperties() {
        return secureVaultProperties;
    }

    public double getFileSizeLimit() {
        return fileSizeLimit;
    }

    public String getMoveAfterMoveFailure() {
        return moveAfterMoveFailure;
    }

    public int getNextRetryDurationForFailedMove() {
        return nextRetryDurationForFailedMove;
    }

    public String getFailedRecordFileName() {
        return failedRecordFileName;
    }

    public String getFailedRecordFileDestination() {
        return failedRecordFileDestination;
    }

    public String getFailedRecordTimestampFormat() {
        return failedRecordTimestampFormat;
    }

    public Integer getFileProcessingInterval() {
        return fileProcessingInterval;
    }

    public Integer getFileProcessingCount() {
        return fileProcessingCount;
    }

    public boolean isAutoLockRelease() {
        return autoLockRelease;
    }

    public Long getAutoLockReleaseInterval() {
        return autoLockReleaseInterval;
    }

    public Boolean getAutoLockReleaseSameNode() {
        return autoLockReleaseSameNode;
    }

    public boolean isDistributedLock() {
        return distributedLock;
    }

    public String getFileSortParam() {
        return fileSortParam;
    }

    public boolean isFileSortAscending() {
        return fileSortAscending;
    }

    public boolean isForceCreateFolder() {
        return forceCreateFolder;
    }

    public String getSubfolderTimestamp() {
        return subfolderTimestamp;
    }

    public Long getDistributedLockTimeout() {
        return distributedLockTimeout;
    }

    public boolean isCanceled() {
        return canceled;
    }

    /**
     * Signal that processing should stop (e.g. on server shutdown). The streaming loop checks this
     * between records/chunks and stops cleanly, so the file's lock is released before the file
     * system manager closes.
     */
    public void setCanceled(boolean canceled) {
        this.canceled = canceled;
    }

    public boolean isResolveHostsDynamically() {
        return resolveHostsDynamically;
    }

    public boolean isFileNotFoundLogged() {
        return fileNotFoundLogged;
    }

    public ParameterInclude getParams() {
        return params;
    }

    public Long getMinimumAge() {
        return minimumAge;
    }

    public Long getMaximumAge() {
        return maximumAge;
    }

    public boolean isBuild() {
        return build;
    }

    public String getReplayFileName() {
        return replayFileName;
    }

    public boolean isAppend() {
        return append;
    }

    public boolean isAvoidPermissionCheck() {
        return avoidPermissionCheck;
    }

    public boolean isPassive() {
        return passive;
    }

    public Long getFailedRecordNextRetryDuration() {
        return failedRecordNextRetryDuration;
    }
}
