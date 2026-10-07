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

import org.apache.axiom.om.OMElement;
import org.apache.axis2.Constants.Configuration;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.format.DataSourceMessageBuilder;
import org.apache.axis2.format.ManagedDataSource;
import org.apache.axis2.format.ManagedDataSourceFactory;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.vfs.VFSConstants;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.transport.customlogsetter.CustomLogSetter;
import org.wso2.carbon.inbound.streaming.StreamingConstants;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;

import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS;

/**
 * Injects an inbound VFS file into a Synapse sequence. Dispatches based on the configured streaming
 * mode: the whole-file (ENTIRE_FILE / non-streaming) path is handled here; CHUNK and RECORD
 * streaming are delegated to {@link StreamInjectHandler}.
 */
public class FileInjectHandler extends AbstractInjectHandler {

    private static final Log log = LogFactory.getLog(FileInjectHandler.class);

    private final FileSystemManager fsManager;

    public FileInjectHandler(String injectingSeq, String onErrorSeq, boolean sequential,
                             SynapseEnvironment synapseEnvironment, VFSConfig vfsProperties,
                             FileSystemManager fsManager) {
        super(injectingSeq, onErrorSeq, sequential, synapseEnvironment, vfsProperties);
        this.fsManager = fsManager;
    }

    /**
     * Entry point for processing a file. Dispatches to the appropriate handler based on the
     * configured streaming mode:
     * <ul>
     *     <li>ENTIRE_FILE (or streaming disabled) - inject the whole file as one message
     *         (previous behaviour, backwards compatible).</li>
     *     <li>CHUNK - stream the file in batches of records, one message per chunk.</li>
     *     <li>RECORD - stream the file record by record, one message per record.</li>
     * </ul>
     */
    public boolean invoke(Object object, String name) {
        FileObject file = (FileObject) object;

        if (vfsProperties.isStreaming()) {
            String mode = vfsProperties.getStreamingMode();
            if (StreamingConstants.STREAMING_MODE_CHUNK.equalsIgnoreCase(mode)) {
                return newStreamHandler().stream(file, name, true);
            } else if (StreamingConstants.STREAMING_MODE_RECORD.equalsIgnoreCase(mode)) {
                return newStreamHandler().stream(file, name, false);
            }
            // ENTIRE_FILE (default) falls through to the whole-file behaviour below.
        }
        return invokeEntireFile(file, name);
    }

    /**
     * Create a {@link StreamInjectHandler} sharing this handler's configuration and the current
     * transport headers / file URI.
     */
    private StreamInjectHandler newStreamHandler() {
        // Streaming forces sequential injection (see StreamInjectHandler) regardless of the
        // inbound's configured 'sequential' setting.
        StreamInjectHandler handler = new StreamInjectHandler(injectingSeq, onErrorSeq,
                synapseEnvironment, vfsProperties, fsManager);
        handler.setTransportHeaders(transportHeaders);
        handler.setFileURI(fileURI);
        return handler;
    }

    /**
     * Inject the whole file as a single message to the sequence.
     */
    private boolean invokeEntireFile(FileObject file, String name) {

        ManagedDataSource dataSource = null;

        InputStream in = null;
        try {
            org.apache.synapse.MessageContext msgCtx = createMessageContext();
            seedInboundProperties(msgCtx, name);

            InboundEndpoint inboundEndpoint = msgCtx.getConfiguration().getInboundEndpoint(name);
            CustomLogSetter.getInstance().setLogAppender(inboundEndpoint.getArtifactContainerName());
            String contentType = vfsProperties.getContentType();
            if (contentType == null || contentType.trim().equals("")) {
                if (file.getName().getExtension().toLowerCase().endsWith("xml")) {
                    contentType = "text/xml";
                } else if (file.getName().getExtension().toLowerCase().endsWith("txt")) {
                    contentType = "text/plain";
                }
            } else {
                // Extract the charset encoding from the configured content type and
                // set the CHARACTER_SET_ENCODING property as e.g. SOAPBuilder relies on this.
                String charSetEnc = null;
                try {
                    charSetEnc = new ContentType(contentType).getParameter("charset");
                } catch (ParseException ex) {
                    // ignore
                }
                msgCtx.setProperty(Configuration.CHARACTER_SET_ENCODING, charSetEnc);
            }
            if (log.isDebugEnabled()) {
                log.debug("Processed file : " + file + " of Content-type : " + contentType);
            }
            MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx)
                    .getAxis2MessageContext();
            // Determine the message builder to use
            Builder builder;
            if (contentType == null) {
                log.debug("No content type specified. Using SOAP builder.");
                builder = new SOAPBuilder();
            } else {
                int index = contentType.indexOf(';');
                String type = index > 0 ? contentType.substring(0, index) : contentType;
                builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
                if (builder == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("No message builder found for type '" + type + "'. Falling back to SOAP.");
                    }
                    builder = new SOAPBuilder();
                }
            }

            // set the message payload to the message context
            boolean isStreaming = vfsProperties.isStreaming();

            if (builder instanceof DataSourceMessageBuilder && isStreaming) {
                dataSource = ManagedDataSourceFactory.create(new FileObjectDataSource(file, contentType));
            } else {
                in = new AutoCloseInputStream(file.getContent().getInputStream());
            }

            //Inject the message to the sequence.

            OMElement documentElement;
            if (in != null) {
                documentElement = builder.processDocument(in, contentType, axis2MsgCtx);
            } else {
                documentElement = ((DataSourceMessageBuilder) builder)
                        .processDocument(dataSource, contentType, axis2MsgCtx);
            }

            if (vfsProperties.isBuild()) {
                documentElement.build();
            }
            msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));

            if (injectingSeq == null || injectingSeq.equals("")) {
                log.error("Sequence name not specified. Sequence : " + injectingSeq);
            }
            SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration()
                    .getSequence(injectingSeq);
            if (seq != null) {
                if (log.isDebugEnabled()) {
                    log.debug("injecting message to sequence : " + injectingSeq);
                }
                if (!seq.isInitialized()) {
                    seq.init(synapseEnvironment);
                }
                seq.setErrorHandler(onErrorSeq);

                // >>> APPEND MODE: ensure any VFS write/Respond appends instead of overwriting
                // This sets the Axis2 property the VFS sender/transport checks.
                if (vfsProperties.isAppend()) {
                    axis2MsgCtx.setProperty("transport.vfs.Append", "true");
                }

                synapseEnvironment.injectInbound(msgCtx, seq, sequential);

                Map<String, Object> responseHeaders =
                        (Map<String, Object>) axis2MsgCtx.getProperty(TRANSPORT_HEADERS);
                String errorCode = (responseHeaders != null)
                        ? (String) responseHeaders.get("ERROR_CODE") : null;
                if (StringUtils.isNotEmpty(errorCode)) {
                    return false;
                }
            } else {
                log.error("Sequence: " + injectingSeq + " not found");
            }
        } catch (Exception e) {
            log.error("Error while processing the file/folder", e);
            return false;
        } finally {
            if (dataSource != null) {
                dataSource.destroy();
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                log.error("Error while closing the input stream", e);
            }
        }
        return true;
    }
}
