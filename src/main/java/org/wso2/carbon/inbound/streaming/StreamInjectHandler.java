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
import com.google.gson.JsonObject;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.inbound.streaming.FailedRecordWriter;
import org.wso2.carbon.inbound.streaming.StreamChunk;
import org.wso2.carbon.inbound.streaming.StreamRecord;
import org.wso2.carbon.inbound.streaming.StreamingConstants;
import org.wso2.carbon.inbound.streaming.StreamingException;
import org.wso2.carbon.inbound.streaming.StreamingProcessor;
import org.wso2.carbon.inbound.streaming.StreamingProcessorFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Streams an inbound VFS file to a Synapse sequence, injecting one message per chunk (CHUNK mode)
 * or per record (RECORD mode) instead of loading the whole file into a single message.
 * <p>
 * Recoverable, per-record parse failures (e.g. a malformed JSONL line) are handled without failing
 * the whole file: the raw bytes of each failed record are siphoned to a sidecar in the configured
 * failed-records folder (see {@link FailedRecordWriter}) and processing continues. If the number of
 * failed records reaches the configured maximum, or the siphon is disabled for the format, the file
 * is treated as a complete failure - the partial sidecar is discarded and the caller applies the
 * configured action-after-failure (move to fault folder / delete) to the whole file.
 * <p>
 * A non-recoverable {@link StreamingException} (structural/IO error) also fails the whole file.
 */
public class StreamInjectHandler extends AbstractInjectHandler {

    private static final Log log = LogFactory.getLog(StreamInjectHandler.class);

    private final FileSystemManager fsManager;
    // Reply file for the source file currently being streamed, or null when no reply file is
    // configured. Opened lazily on the first mediated result and closed when the file is done.
    private ReplyFileWriter replyWriter;

    public StreamInjectHandler(String injectingSeq, String onErrorSeq,
        SynapseEnvironment synapseEnvironment, VFSConfig vfsProperties,
        FileSystemManager fsManager) {
        // Streaming always injects sequentially. Per-record/per-chunk mediation errors can only be
        // detected synchronously - the ERROR_CODE transport header is set while the sequence runs -
        // which requires injectInbound to mediate inline rather than hand off asynchronously.
        super(injectingSeq, onErrorSeq, true, synapseEnvironment, vfsProperties);
        this.fsManager = fsManager;
    }

    /**
     * Stream a file to the sequence, injecting one message per chunk or per record.
     *
     * @param file      the file being processed
     * @param name      the inbound endpoint name
     * @param chunkMode true for CHUNK mode (ChunkIterator), false for RECORD mode (RowIterator)
     * @return true if the file was processed successfully (bad records may have been siphoned);
     * false if the file must be treated as a complete failure
     */
    public boolean stream(FileObject file, String name, boolean chunkMode) {
        // In streaming modes the content type is fixed by the input format (not the user-configured
        // transport.vfs.ContentType), and drives both the reader charset and the message builder.
        String contentType = vfsProperties.getStreamingContentType();
        String inputFormat = vfsProperties.getStreamingInputFormat();
        StreamingProcessor processor = StreamingProcessorFactory.getProcessor(inputFormat,
            vfsProperties);
        if (processor == null) {
            log.error("No streaming processor found for input format '" + inputFormat
                + "'. Cannot stream file: " + file.getName().getBaseName());
            return false;
        }

        boolean addOutputToProperty = vfsProperties.isStreamingAddOutputToProperty();
        FailedRecordCollector failed = new FailedRecordCollector(file, name);
        this.replyWriter = openReplyWriter(file);

        // Checkpointing: resume from the last confirmed record if a matching checkpoint exists.
        StreamingCheckpointManager checkpoints = openCheckpoints(file, name);
        long startFromRecord = 0;
        long startFromChunk = 0;
        if (checkpoints != null) {
            StreamingCheckpoint cp = checkpoints.loadResumable();
            if (cp != null) {
                startFromRecord = cp.getRecordsConsumed();
                startFromChunk = cp.getChunksConsumed();
                failed.seed(cp.getProcessedRecords(), cp.getParseFailedRecords(),
                    cp.getMediationFailedRecords());
                log.info("Resuming streaming of " + file.getName().getBaseName()
                    + " from record " + startFromRecord + " (checkpoint).");
            }
        }
        long resumedFrom = startFromRecord;
        int interval = vfsProperties.getStreamingCheckpointInterval();
        long lastConsumed = startFromRecord;
        // Absolute chunk number of the last chunk delivered, so a resume keeps counting.
        long lastChunk = startFromChunk;
        boolean cancelled = false;

        try (InputStream in = file.getContent().getInputStream()) {
            if (chunkMode) {
                Iterator<StreamChunk> iterator = processor.getChunkIterator(in, contentType,
                    vfsProperties.getStreamingChunkSize(), startFromRecord, startFromChunk);
                int unitsSinceFlush = 0;
                while (iterator.hasNext()) {
                    // Stop cleanly on server shutdown, at a chunk boundary.
                    if (vfsProperties.isCanceled()) {
                        cancelled = true;
                        break;
                    }
                    try {
                        StreamChunk chunk = iterator.next();
                        handleChunk(name, contentType, chunk, addOutputToProperty, failed, processor);
                        lastConsumed = chunk.getLastRecordNumber();
                        lastChunk = chunk.getChunkNumber();
                        if (checkpoints != null && ++unitsSinceFlush >= interval) {
                            saveCheckpoint(checkpoints, lastConsumed, lastChunk, failed);
                            unitsSinceFlush = 0;
                        }
                    } catch (StreamingException ex) {
                        if (vfsProperties.isCanceled()) {
                            cancelled = true;
                            break;
                        }
                        if (!handleStreamingException(ex, file, failed, "chunk")) {
                            failed.fileFailed();
                            clearCheckpoints(checkpoints);
                            onFileFailure(file, name, failed);
                            return false;
                        }
                    }
                }
            } else {
                Iterator<StreamRecord> iterator = processor.getRecordIterator(in, contentType,
                    startFromRecord);
                int unitsSinceFlush = 0;
                while (iterator.hasNext()) {
                    // Stop cleanly on server shutdown, at a record boundary.
                    if (vfsProperties.isCanceled()) {
                        cancelled = true;
                        break;
                    }
                    try {
                        StreamRecord record = iterator.next();
                        handleRecord(name, contentType, record, addOutputToProperty, failed);
                        lastConsumed = record.getRecordNumber();
                        if (checkpoints != null && ++unitsSinceFlush >= interval) {
                            saveCheckpoint(checkpoints, lastConsumed, lastChunk, failed);
                            unitsSinceFlush = 0;
                        }
                    } catch (StreamingException ex) {
                        if (vfsProperties.isCanceled()) {
                            cancelled = true;
                            break;
                        }
                        if (!handleStreamingException(ex, file, failed, "record")) {
                            failed.fileFailed();
                            clearCheckpoints(checkpoints);
                            onFileFailure(file, name, failed);
                            return false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (vfsProperties.isCanceled()) {
                // Shutdown race: the read failed because the file system manager was closed
                // mid-stream. Treat it as a graceful stop, not a file failure.
                cancelled = true;
            } else {
                log.error("Error while streaming the file/folder : " + file.getName(), e);
                failed.discard();
                failed.fileFailed();
                clearCheckpoints(checkpoints);
                onFileFailure(file, name, failed);
                return false;
            }
        } finally {
            failed.close();
            closeReplyWriter(file);
        }

        if (cancelled) {
            return finishOnShutdown(file, checkpoints, lastConsumed, lastChunk, failed);
        }

        // Success: the file has been fully consumed, so the checkpoint is no longer needed.
        clearCheckpoints(checkpoints);
        failed.fileCompleted();
        log.info("Streaming completed for " + file.getName().getBaseName()
            + ": processed=" + failed.getProcessedCount()
            + ", parseFailed=" + failed.getParseFailedCount()
            + ", mediationFailed=" + failed.getMediationFailedCount()
            + (resumedFrom > 0 ? " (resumed from record " + resumedFrom + ")" : ""));
        onFileComplete(file, name, failed);
        return true;
    }

    /**
     * Handle a shutdown-interrupted stream: save a checkpoint at the last confirmed record (so the
     * file resumes on restart) and leave the file in place. The checkpoint is NOT cleared. Returns
     * true; the caller ({@code VFSConsumer}) sees the cancel flag and skips success/fail
     * post-processing, leaving the file to be re-polled.
     * <p>
     * Neither completion callback fires here: the file is not finished, it is paused.
     */
    private boolean finishOnShutdown(FileObject file, StreamingCheckpointManager checkpoints,
                                     long lastConsumed, long lastChunk,
                                     FailedRecordCollector failed) {
        String base = file.getName().getBaseName();
        if (checkpoints != null) {
            log.info("Streaming of " + base + " interrupted by server shutdown after record "
                + lastConsumed + " (processed=" + failed.getProcessedCount()
                + ", parseFailed=" + failed.getParseFailedCount()
                + ", mediationFailed=" + failed.getMediationFailedCount()
                + "). Saving a checkpoint so processing resumes from record " + lastConsumed
                + " on restart; the file is left in place.");
            saveCheckpoint(checkpoints, lastConsumed, lastChunk, failed);
        } else {
            log.info("Streaming of " + base + " interrupted by server shutdown (checkpointing "
                + "disabled); the file is left in place and will be reprocessed from the start on "
                + "restart.");
        }
        return true;
    }

    /**
     * Build a checkpoint manager for this file, or {@code null} if checkpointing is disabled or the
     * file fingerprint / registry is unavailable (processing then proceeds without resume support).
     */
    private StreamingCheckpointManager openCheckpoints(FileObject file, String name) {
        if (!vfsProperties.isStreamingCheckpointEnabled()) {
            return null;
        }
        try {
            return new StreamingCheckpointManager(synapseEnvironment, vfsProperties, name, file);
        } catch (Exception e) {
            log.warn("Could not initialize checkpointing for " + file.getName().getBaseName()
                + "; processing without resume support.", e);
            return null;
        }
    }

    private void saveCheckpoint(StreamingCheckpointManager checkpoints, long recordsConsumed,
                                long chunksConsumed, FailedRecordCollector failed) {
        checkpoints.save(recordsConsumed, chunksConsumed, failed.getProcessedCount(),
            failed.getParseFailedCount(), failed.getMediationFailedCount(),
            System.currentTimeMillis());
    }

    private void clearCheckpoints(StreamingCheckpointManager checkpoints) {
        if (checkpoints != null) {
            checkpoints.clear();
        }
    }

    /**
     * Where a streamed message sits in its file, stamped onto the message context so the sequence
     * can tell which rows it is holding. Independent of where the parsed output went: the raw-body
     * path gets these properties just as the output-property path does.
     */
    private static final class StreamPosition {

        private final Long recordNumber;    // RECORD mode only
        private final Integer chunkNumber;  // CHUNK mode only
        private final Long firstRecord;
        private final Long lastRecord;
        private final Integer recordsInChunk;

        private StreamPosition(Long recordNumber, Integer chunkNumber, Long firstRecord,
                               Long lastRecord, Integer recordsInChunk) {
            this.recordNumber = recordNumber;
            this.chunkNumber = chunkNumber;
            this.firstRecord = firstRecord;
            this.lastRecord = lastRecord;
            this.recordsInChunk = recordsInChunk;
        }

        static StreamPosition ofRecord(StreamRecord record) {
            return new StreamPosition(record.getRecordNumber(), null, null, null, null);
        }

        static StreamPosition ofChunk(StreamChunk chunk, int validRecords) {
            return new StreamPosition(null, chunk.getChunkNumber(), chunk.getFirstRecordNumber(),
                chunk.getLastRecordNumber(), validRecords);
        }

        /** Stamp the position onto the message, as Strings like the inbound's other properties. */
        void applyTo(org.apache.synapse.MessageContext msgCtx) {
            if (recordNumber != null) {
                msgCtx.setProperty(StreamingConstants.POS_RECORD_NUMBER,
                    String.valueOf(recordNumber));
            }
            if (chunkNumber != null) {
                msgCtx.setProperty(StreamingConstants.POS_CHUNK_NUMBER,
                    String.valueOf(chunkNumber));
                msgCtx.setProperty(StreamingConstants.POS_FIRST_RECORD,
                    String.valueOf(firstRecord));
                msgCtx.setProperty(StreamingConstants.POS_LAST_RECORD,
                    String.valueOf(lastRecord));
                msgCtx.setProperty(StreamingConstants.POS_CHUNK_SIZE,
                    String.valueOf(recordsInChunk));
            }
        }
    }

    /** Fire the completion callback, if one is configured, after a file finished successfully. */
    private void onFileComplete(FileObject file, String name, FailedRecordCollector failed) {
        invokeFileCallback(vfsProperties.getStreamingFileCompleteSequence(), file, name,
            StreamingConstants.CB_STATUS_COMPLETED, failed);
    }

    /** Fire the failure callback, if one is configured, after a whole-file failure. */
    private void onFileFailure(FileObject file, String name, FailedRecordCollector failed) {
        invokeFileCallback(vfsProperties.getStreamingFileFailureSequence(), file, name,
            StreamingConstants.CB_STATUS_FAILED, failed);
    }

    /**
     * Invoke a once-per-file callback sequence with the file's totals, as a JSON summary body plus
     * discrete properties.
     * <p>
     * The outcome of the file is already decided by the time this runs: a callback that fails is
     * logged and otherwise ignored, so a broken callback sequence cannot turn a good file bad or
     * re-trigger itself. It is injected inline, so post-processing (the move to the done/fault
     * folder) happens only after the callback has returned.
     */
    private void invokeFileCallback(String seqName, FileObject file, String name, String status,
                                    FailedRecordCollector failed) {
        if (StringUtils.isBlank(seqName)) {
            return;
        }
        String base = file.getName().getBaseName();
        try {
            JsonObject summary = new JsonObject();
            summary.addProperty("file", base);
            summary.addProperty("uri", Utils.maskURLPassword(file.getName().getURI()));
            summary.addProperty("status", status);
            summary.addProperty("processed", failed.getProcessedCount());
            summary.addProperty("parseFailed", failed.getParseFailedCount());
            summary.addProperty("mediationFailed", failed.getMediationFailedCount());

            org.apache.synapse.MessageContext msgCtx = createMessageContext();
            seedInboundProperties(msgCtx, name);
            MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();

            msgCtx.setProperty(StreamingConstants.CB_FILE_NAME, base);
            msgCtx.setProperty(StreamingConstants.CB_FILE_URI,
                Utils.maskURLPassword(file.getName().getURI()));
            msgCtx.setProperty(StreamingConstants.CB_STATUS, status);
            msgCtx.setProperty(StreamingConstants.CB_PROCESSED,
                String.valueOf(failed.getProcessedCount()));
            msgCtx.setProperty(StreamingConstants.CB_PARSE_FAILED,
                String.valueOf(failed.getParseFailedCount()));
            msgCtx.setProperty(StreamingConstants.CB_MEDIATION_FAILED,
                String.valueOf(failed.getMediationFailedCount()));

            String json = summary.toString();
            Builder builder = resolveBuilder(StreamingConstants.STREAMING_CONTENT_TYPE_JSON,
                axis2MsgCtx);
            OMElement documentElement = builder.processDocument(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),
                StreamingConstants.STREAMING_CONTENT_TYPE_JSON, axis2MsgCtx);
            msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));

            if (injectToNamedSequence(seqName, msgCtx, axis2MsgCtx)) {
                log.info("Invoked " + status + " callback sequence '" + seqName + "' for " + base);
            } else {
                log.warn("Callback sequence '" + seqName + "' reported an error for " + base
                    + ". The file outcome (" + status + ") is unchanged.");
            }
        } catch (Exception e) {
            log.error("Callback sequence '" + seqName + "' failed for " + base
                + ". The file outcome (" + status + ") is unchanged.", e);
        }
    }

    /**
     * Process a single record: siphon it if invalid, otherwise inject it.
     *
     * @return false if the whole file must be treated as a complete failure
     */
    private boolean handleRecord(String name, String contentType, StreamRecord record,
        boolean addOutputToProperty, FailedRecordCollector failed) throws Exception {
        if (!record.isValid()) {
            failed.parseFailure(record);
            return true;
        }
        byte[] body = addOutputToProperty ? null : record.getContent();
        if (tryInject(name, contentType, body, payloadText(record.getJSONPayload()),
                StreamPosition.ofRecord(record), "record " + record.getRecordNumber())) {
            failed.recordProcessed(1);
        } else {
            // Mediation failed: apply the mediation-error action to this record's content (raw
            // content, or its payload in property-output mode).
            log.warn("Record " + record.getRecordNumber() + " failed mediation; "
                + failed.mediationActionLabel() + " the record.");
            byte[] failBytes = addOutputToProperty
                    ? payloadBytes(record.getJSONPayload(), record.getEncoding())
                    : record.getContent();
            failed.mediationFailure(failBytes, 1);
        }
        return true;
    }

    /**
     * Process a single chunk: apply the parse-error action to its invalid records, then inject the
     * valid ones as one batch.
     * <p>
     * If the batch fails mediation we do <em>not</em> retry it record-by-record: in chunk mode the
     * sequence is written against the batch shape (e.g. expressions over the JSON array), so a lone
     * record would not mediate correctly. Instead the whole chunk is handled by the mediation-error
     * action in the same shape it was sent (a JSON array for JSON/JSONL, newline-joined lines for
     * text/CSV).
     */
    private boolean handleChunk(String name, String contentType, StreamChunk chunk,
        boolean addOutputToProperty, FailedRecordCollector failed,
        StreamingProcessor processor) throws Exception {
        List<StreamRecord> validRecords = new ArrayList<>();
        for (StreamRecord record : chunk.getRecords()) {
            if (record.isValid()) {
                validRecords.add(record);
            } else {
                failed.parseFailure(record);
            }
        }
        // Nothing left to inject once the invalid records have been handled.
        if (validRecords.isEmpty()) {
            return true;
        }

        // The processor knows how to combine its own records (newline-joined text, JSON array, ...).
        byte[] body = addOutputToProperty ? null : processor.buildChunkBody(chunk);
        if (tryInject(name, contentType, body, payloadText(chunk.getJSONPayload()),
                StreamPosition.ofChunk(chunk, validRecords.size()),
                describe(validRecords) + " (chunk " + chunk.getChunkNumber() + ")")) {
            failed.recordProcessed(validRecords.size());
        } else {
            // The chunk failed mediation as a unit; hand the whole chunk (in the shape it was sent)
            // to the mediation-error action.
            log.warn("Chunk " + chunk.getChunkNumber() + " failed mediation; "
                + failed.mediationActionLabel() + " its " + validRecords.size() + " record(s).");
            byte[] failBytes = addOutputToProperty
                    ? payloadBytes(chunk.getJSONPayload(), chunk.getEncoding())
                    : body;
            failed.mediationFailure(failBytes, validRecords.size());
        }
        return true;
    }

    /**
     * Serialize a JSON payload to the text placed in the output property, or null if there is no
     * payload. {@code toString()} - not {@code getAsString()} - because a record payload is a JSON
     * object and a chunk payload a JSON array; {@code getAsString()} is only defined for a single
     * primitive and throws for both of those.
     */
    private static String payloadText(JsonElement payload) {
        return payload != null ? payload.toString() : null;
    }

    /** Serialize a JSON payload to bytes, or null if there is no payload. */
    private static byte[] payloadBytes(JsonElement payload, Charset charset) {
        return payload != null ? payload.toString().getBytes(charset) : null;
    }

    /**
     * Handle a StreamingException thrown mid-iteration. Recoverable errors are logged and skipped;
     * non-recoverable errors fail the whole file (discarding any partial sidecar).
     *
     * @return true to continue processing, false to abort the file
     */
    private boolean handleStreamingException(StreamingException ex, FileObject file,
        FailedRecordCollector failed, String unit) {
        if (ex.isRecoverable()) {
            log.warn("Recoverable streaming error at row " + ex.getRowNumber()
                + ". Continuing with next " + unit + ".", ex);
            return true;
        }
        // A malformed record makes the rest of the stream un-parseable for single-document formats
        // (CSV/JSON), so we still abort the whole file - but the record that broke parsing is a
        // genuine parse failure, so count it (visible in the parse_failed metric for every format,
        // not just JSONL) before failing the file.
        if (ex.getKind() == StreamingException.Kind.PARSE) {
            failed.recordFatalParseError(ex.getRowNumber());
        }
        log.error("Unrecoverable streaming error at row " + ex.getRowNumber()
            + ". Aborting processing the file : " + file.getName(), ex);
        failed.discard();
        return false;
    }

    /**
     * Create a message context for a single chunk/record and inject it to the sequence. When
     * {@code body} is non-null it becomes the message payload; when {@code propertyOutput} is
     * non-null it is exposed as a message-context property and the body is left empty.
     *
     * @return true if the injection completed without an error code
     */
    private boolean injectStreamingMessage(String name, String contentType, byte[] body,
        String propertyOutput, StreamPosition position) throws Exception {
        org.apache.synapse.MessageContext msgCtx = createMessageContext();
        seedInboundProperties(msgCtx, name);
        MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();
        // Before the payload, so the position is there whichever output path runs below.
        position.applyTo(msgCtx);

        if (vfsProperties.isStreamingAddOutputToProperty()) {
            msgCtx.setProperty(vfsProperties.getStreamingOutputProperty(), propertyOutput);
            // add empty SOAP envelope.
            SOAPEnvelope envelope = OMAbstractFactory.getSOAP12Factory().getDefaultEnvelope();
            msgCtx.setEnvelope(envelope);
        } else {
            // Raw record/chunk content becomes the message body.
            Builder builder = resolveBuilder(contentType, axis2MsgCtx);
            OMElement documentElement = builder.processDocument(
                new ByteArrayInputStream(body), contentType, axis2MsgCtx);
            if (vfsProperties.isBuild()) {
                documentElement.build();
            }
            msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));
        }
        boolean mediated = injectToSequence(name, msgCtx, axis2MsgCtx);
        if (mediated && replyWriter != null) {
            // Only successful results reach the reply file; a failed record is siphoned to the
            // mediation-error sidecar instead, so it never appears in both.
            replyWriter.append(axis2MsgCtx);
        }
        return mediated;
    }

    /**
     * Open the reply file for this source file, or return null when transport.vfs.ReplyFileURI is
     * not configured (the common case - no reply file is written at all).
     */
    private ReplyFileWriter openReplyWriter(FileObject file) {
        String replyFileUri = vfsProperties.getReplyFileURI();
        if (StringUtils.isBlank(replyFileUri) || fsManager == null) {
            return null;
        }
        return new ReplyFileWriter(fsManager, vfsProperties, replyFileUri,
            vfsProperties.getReplayFileName(), file.getName().getBaseName());
    }

    private void closeReplyWriter(FileObject file) {
        if (replyWriter == null) {
            return;
        }
        if (replyWriter.getWrittenCount() > 0) {
            log.info("Appended " + replyWriter.getWrittenCount() + " mediated result(s) from "
                + file.getName().getBaseName() + " to the reply file.");
        }
        replyWriter.close();
        replyWriter = null;
    }

    /**
     * Attempt to inject one message (a single record, or a chunk batch). A mediation failure is the
     * injection returning an error code or throwing; unlike a parse failure the data itself is fine
     * (the downstream sequence failed). This method only reports success/failure - the caller
     * decides what to siphon - so a failed chunk can be retried record-by-record.
     *
     * @param what a short description of the unit, for logging
     * @return true if mediation succeeded, false on a mediation failure
     */
    private boolean tryInject(String name, String contentType, byte[] body,
                              String propertyOutput, StreamPosition position, String what) {
        try {
            if (injectStreamingMessage(name, contentType, body, propertyOutput, position)) {
                return true;
            }
            log.warn("Mediation reported an error for " + what + ".");
            return false;
        } catch (Exception e) {
            log.warn("Mediation error while injecting " + what + ".", e);
            return false;
        }
    }

    private static String describe(List<StreamRecord> records) {
        if (records.isEmpty()) {
            return "no records";
        }
        long first = records.get(0).getRecordNumber();
        long last = records.get(records.size() - 1).getRecordNumber();
        return first == last ? ("record " + first) : ("records " + first + "-" + last);
    }

    /**
     * Applies the configured per-record error actions for a single source file across two
     * independent kinds of failure:
     * <ul>
     *     <li><b>parse errors</b> - bad data (only JSONL reaches this per-record). The
     *     {@code StreamingParseErrorAction} decides MOVE (append to the {@code .parse.fail} sidecar)
     *     or DROP (discard); either way processing continues.</li>
     *     <li><b>mediation errors</b> - downstream failures. The {@code StreamingMediationErrorAction}
     *     decides MOVE (append to the {@code .mediation.fail} sidecar) or DROP; either way processing
     *     continues.</li>
     * </ul>
     * Neither kind fails the whole file - that only happens on a non-recoverable structural/IO error.
     */
    private final class FailedRecordCollector {

        private final FileObject sourceFile;
        private final boolean parseMove;
        private final boolean mediationMove;
        private final FailedRecordWriter parseWriter;
        private final FailedRecordWriter mediationWriter;
        private long processedCount;
        private long parseFailedCount;
        private long mediationFailedCount;

        FailedRecordCollector(FileObject sourceFile, String inboundName) {
            this.sourceFile = sourceFile;
            this.parseMove = StreamingConstants.STREAMING_ERROR_ACTION_MOVE
                    .equalsIgnoreCase(vfsProperties.getStreamingParseErrorAction());
            this.mediationMove = StreamingConstants.STREAMING_ERROR_ACTION_MOVE
                    .equalsIgnoreCase(vfsProperties.getStreamingMediationErrorAction());
            // Parse errors: dedicated folder, then the fault folder.
            this.parseWriter = (parseMove && fsManager != null)
                    ? buildWriter(sourceFile, "parse.fail", "parse-error",
                        vfsProperties.getStreamingParseErrorFolder(),
                        vfsProperties.getMoveAfterFailure())
                    : null;
            // Mediation errors: dedicated folder, then the parse folder, then the fault folder.
            this.mediationWriter = (mediationMove && fsManager != null)
                    ? buildWriter(sourceFile, "mediation.fail", "mediation-error",
                        vfsProperties.getStreamingMediationErrorFolder(),
                        vfsProperties.getStreamingParseErrorFolder(),
                        vfsProperties.getMoveAfterFailure())
                    : null;
        }

        /** Build a writer targeting the first non-empty folder in {@code candidates}, or null. */
        private FailedRecordWriter buildWriter(FileObject sourceFile, String marker, String label,
                                               String... candidates) {
            String folder = firstNonEmpty(candidates);
            if (folder == null) {
                log.warn("Streaming " + label + " action is MOVE but no destination folder (or "
                    + "fault folder) is configured; " + label + " records will be logged only.");
                return null;
            }
            return new FailedRecordWriter(fsManager, vfsProperties, folder,
                sourceFile.getName().getBaseName(), marker);
        }

        private String firstNonEmpty(String... values) {
            for (String v : values) {
                if (v != null && !v.trim().isEmpty()) {
                    return v;
                }
            }
            return null;
        }

        /**
         * Apply the parse-error action to an invalid (unparseable) record: MOVE appends it to the
         * parse-error sidecar, DROP discards it. Processing always continues.
         */
        void parseFailure(StreamRecord record) {
            parseFailedCount++;
            log.warn("Invalid record at row " + record.getRecordNumber() + " in file "
                + sourceFile.getName().getBaseName()
                + (record.getParseError() != null ? " : " + record.getParseError() : ""));
            if (parseWriter != null) {
                parseWriter.append(record.getContent());
            }
        }

        /**
         * Record the single record whose parse error is fatal for the file (CSV/JSON), so that a
         * parse failure is counted even though the file cannot continue. The whole file is still
         * failed by the caller; nothing is siphoned (the file itself goes to the fault folder).
         */
        void recordFatalParseError(long rowNumber) {
            parseFailedCount++;
            log.warn("Fatal parse error at row " + rowNumber + " in file "
                + sourceFile.getName().getBaseName() + "; the file will be treated as failed.");
        }

        /**
         * Apply the mediation-error action to one injection unit (a single record, or a whole
         * chunk): MOVE appends the pre-formatted {@code content} (in the shape it was sent) to the
         * mediation-error sidecar, DROP discards it. Processing always continues.
         *
         * @param content     the bytes to move, in the shape the unit was mediated
         * @param recordCount the number of records the unit represents (for reporting)
         */
        void mediationFailure(byte[] content, long recordCount) {
            mediationFailedCount += recordCount;
            if (mediationWriter != null && content != null) {
                mediationWriter.append(content);
            }
        }

        /** "moving" or "dropping", for log messages. */
        String mediationActionLabel() {
            return mediationMove ? "moving" : "dropping";
        }

        /** Count records that were successfully injected. */
        void recordProcessed(long count) {
            processedCount += count;
        }

        /** Seed the counters from a resumed checkpoint so the totals stay absolute. */
        void seed(long processed, long parseFailed, long mediationFailed) {
            this.processedCount = processed;
            this.parseFailedCount = parseFailed;
            this.mediationFailedCount = mediationFailed;
        }

        /** One file processed to completion (recorded once per file). */
        void fileCompleted() {
        }

        /** One file treated as a whole-file failure (recorded once per file). */
        void fileFailed() {
        }

        long getProcessedCount() {
            return processedCount;
        }

        long getParseFailedCount() {
            return parseFailedCount;
        }

        long getMediationFailedCount() {
            return mediationFailedCount;
        }

        void discard() {
            if (parseWriter != null) {
                parseWriter.discard();
            }
            if (mediationWriter != null) {
                mediationWriter.discard();
            }
        }

        void close() {
            if (parseWriter != null) {
                if (parseFailedCount > 0) {
                    log.info("Moved " + parseWriter.getWrittenCount() + " parse-error record(s) "
                        + "from " + sourceFile.getName().getBaseName() + " to the parse-error "
                        + "folder.");
                }
                parseWriter.close();
            }
            if (mediationWriter != null) {
                if (mediationFailedCount > 0) {
                    log.info("Moved " + mediationWriter.getWrittenCount() + " mediation-error "
                        + "record(s) from " + sourceFile.getName().getBaseName() + " to the "
                        + "mediation-error folder.");
                }
                mediationWriter.close();
            }
        }
    }
}
