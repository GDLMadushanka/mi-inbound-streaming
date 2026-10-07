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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.util.MessageProcessorSelector;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.synapse.commons.json.JsonUtil;

/**
 * Appends the mediated result of each streamed chunk/record to the reply file configured by
 * {@code transport.vfs.ReplyFileURI}.
 * <p>
 * One streamed source file produces many messages, so the reply file is opened once for the
 * lifetime of that source file and every result is <em>appended</em> to it. Append is not a choice
 * here (there is no {@code transport.vfs.Append} switch): overwriting would leave only the last
 * chunk's output, so it is the only semantic that makes sense for a stream.
 * <p>
 * The payload is written the way the runtime already writes VFS replies:
 * <ul>
 *     <li><b>JSON</b> - detected with {@link JsonUtil#hasAJsonPayload(MessageContext)}, written with
 *     {@link JsonUtil#writeAsJson(MessageContext, OutputStream)} followed by a newline, so the
 *     result is JSON Lines: one self-contained JSON document per record, appendable without
 *     rewriting a wrapper array and re-readable by this same inbound in {@code jsonl} mode.</li>
 *     <li><b>Anything else</b> - handed to the Axis2 {@link MessageFormatter} chosen for the
 *     message, exactly as {@code VFSTransportSender.populateResponseFile} does
 *     ({@code MessageProcessorSelector.getMessageFormatter} + {@code BaseUtils.getOMOutputFormat} +
 *     {@code formatter.writeTo(..., preserve = true)}), so an XML reply is byte-for-byte what a
 *     {@code <send>} to a VFS endpoint would have produced.</li>
 * </ul>
 * Because the type is only known once a message has been mediated, the destination is resolved on
 * the first append; that is also what decides the default file extension.
 * <p>
 * Nothing here can fail a file. If the destination cannot be opened, or a write fails, the writer
 * logs once and degrades to a no-op so streaming continues.
 */
public class ReplyFileWriter implements Closeable {

    private static final Log log = LogFactory.getLog(ReplyFileWriter.class);
    private static final byte[] NEWLINE = System.lineSeparator().getBytes();

    /** Default reply file name when {@code transport.vfs.ReplyFileName} is not configured. */
    static final String DEFAULT_JSON_REPLY_FILE = "response.jsonl";
    static final String DEFAULT_XML_REPLY_FILE = VFSConstants.DEFAULT_RESPONSE_FILE;

    private final FileSystemManager fsManager;
    private final VFSConfig vfsConfig;
    private final String replyFileUri;
    private final String replyFileName;
    private final String sourceBaseName;

    private boolean initialized;
    private boolean emptyBodyWarned;
    private FileObject destFile;
    private OutputStream out;
    private long written;

    /**
     * @param fsManager      the VFS manager used to resolve the destination
     * @param vfsConfig      the inbound VFS configuration (for scheme options)
     * @param replyFileUri   VFS URI of the reply folder (must be non-empty)
     * @param replyFileName  reply file name, or null/empty to derive one from the payload type
     * @param sourceBaseName base name of the source file, for logging
     */
    public ReplyFileWriter(FileSystemManager fsManager, VFSConfig vfsConfig, String replyFileUri,
                           String replyFileName, String sourceBaseName) {
        this.fsManager = fsManager;
        this.vfsConfig = vfsConfig;
        this.replyFileUri = replyFileUri;
        this.replyFileName = replyFileName;
        this.sourceBaseName = sourceBaseName;
    }

    /**
     * Append one mediated message to the reply file, opening it on first use.
     * <p>
     * Never throws: a write failure is logged once and the writer goes quiet, so a broken reply
     * destination cannot fail the records that were already delivered to the sequence.
     *
     * @param axis2MsgCtx the Axis2 message context <em>after</em> mediation
     */
    public void append(MessageContext axis2MsgCtx) {
        if (axis2MsgCtx == null) {
            return;
        }
        boolean json = isJson(axis2MsgCtx);
        if (!json && isEmptyBody(axis2MsgCtx)) {
            // In property-output mode the message starts life as an empty SOAP envelope and the
            // parsed record sits in a property. If the sequence never builds a payload there is
            // nothing to reply with - appending one empty envelope per record would fill the reply
            // file with noise - so skip it and say so once.
            warnEmptyBodyOnce();
            return;
        }
        ensureOpen(json);
        if (out == null) {
            return;
        }
        try {
            if (json) {
                // One JSON document per line (JSON Lines) - see the class javadoc.
                JsonUtil.writeAsJson(axis2MsgCtx, out);
                out.write(NEWLINE);
            } else {
                MessageFormatter formatter = MessageProcessorSelector.getMessageFormatter(axis2MsgCtx);
                if (formatter == null) {
                    log.error("No Axis2 message formatter for the mediated result of '"
                        + sourceBaseName + "'. Nothing further will be written to the reply file.");
                    closeQuietly();
                    out = null;
                    return;
                }
                OMOutputFormat format = BaseUtils.getOMOutputFormat(axis2MsgCtx);
                // preserve = true: do not consume the envelope, the caller still owns the context.
                formatter.writeTo(axis2MsgCtx, format, out, true);
                out.write(NEWLINE);
            }
            out.flush();
            written++;
        } catch (Exception e) {
            log.error("Failed to append a mediated result to the reply file for '" + sourceBaseName
                + "'. Subsequent results for this file will not be written.", e);
            closeQuietly();
            out = null;
        }
    }

    private boolean isEmptyBody(MessageContext axis2MsgCtx) {
        try {
            SOAPEnvelope envelope = axis2MsgCtx.getEnvelope();
            return envelope == null || envelope.getBody() == null
                || envelope.getBody().getFirstElement() == null;
        } catch (Exception e) {
            return false;
        }
    }

    private void warnEmptyBodyOnce() {
        if (!emptyBodyWarned) {
            emptyBodyWarned = true;
            log.warn("A reply file is configured (transport.vfs.ReplyFileURI) but the sequence "
                + "left the message body empty while streaming '" + sourceBaseName + "'. Nothing "
                + "is appended for such records. In property-output mode the parsed record is in "
                + "the '" + vfsConfig.getStreamingOutputProperty() + "' property, so the sequence "
                + "has to build a payload from it for there to be a reply. Reported once per file.");
        }
    }

    private boolean isJson(MessageContext axis2MsgCtx) {
        try {
            // MI 4.1.0's Synapse names this hasAJsonPayload; later releases add hasJsonPayload.
            return JsonUtil.hasAJsonPayload(axis2MsgCtx);
        } catch (Exception e) {
            log.debug("Could not determine whether the mediated result is JSON; "
                + "falling back to the message formatter.", e);
            return false;
        }
    }

    private void ensureOpen(boolean json) {
        if (initialized) {
            return;
        }
        initialized = true;
        try {
            Map<String, String> query = Utils.parseSchemeFileOptions(replyFileUri, new Properties());
            if (query != null) {
                query.putAll(vfsConfig.getVfsSchemeProperties());
            }
            String location = Utils.extractPath(Utils.stripVfsSchemeIfPresent(replyFileUri));
            FileSystemOptions fso = Utils.attachFileSystemOptions(query, fsManager);
            FileObject folder = fsManager.resolveFile(location, fso);
            folder.createFolder();
            destFile = folder.resolveFile(resolveReplyFileName(json));
            // Append: one source file produces many messages, and repeated runs accumulate.
            out = destFile.getContent().getOutputStream(true);
            log.info("Streaming replies for " + sourceBaseName + " are being appended to '"
                + Utils.maskURLPassword(destFile.getName().getURI()) + "'.");
        } catch (Exception e) {
            log.error("Could not open the reply file for '" + sourceBaseName + "' in '"
                + Utils.maskURLPassword(replyFileUri) + "'. Mediated results will not be written.", e);
            closeQuietly();
            out = null;
        }
    }

    /**
     * The configured name, or a default chosen from the payload type of the first mediated result.
     */
    private String resolveReplyFileName(boolean json) {
        if (StringUtils.isNotBlank(replyFileName)) {
            return replyFileName.trim();
        }
        return json ? DEFAULT_JSON_REPLY_FILE : DEFAULT_XML_REPLY_FILE;
    }

    /** @return the number of mediated results appended to the reply file so far. */
    public long getWrittenCount() {
        return written;
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                log.debug("Error closing the reply file stream", e);
            }
        }
        if (destFile != null) {
            try {
                destFile.close();
            } catch (Exception e) {
                log.debug("Error closing the reply file object", e);
            }
        }
    }
}
