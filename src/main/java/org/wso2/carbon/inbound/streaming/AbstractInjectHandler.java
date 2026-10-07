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

import org.apache.axiom.om.util.UUIDGenerator;
import org.apache.axis2.AxisFault;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.context.MessageContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.base.SequenceMediator;

import java.util.HashMap;
import java.util.Map;

import static org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS;

/**
 * Shared plumbing for injecting messages built from an inbound VFS file into a Synapse sequence.
 * <p>
 * {@link FileInjectHandler} injects the whole file as a single message; {@link StreamInjectHandler}
 * injects one message per streamed chunk/record. Both need the same message-context creation,
 * message-builder selection and sequence-injection boilerplate, which lives here.
 */
public abstract class AbstractInjectHandler {

    private static final Log log = LogFactory.getLog(AbstractInjectHandler.class);

    protected final String injectingSeq;
    protected final String onErrorSeq;
    protected final boolean sequential;
    protected final SynapseEnvironment synapseEnvironment;
    protected final VFSConfig vfsProperties;
    protected Map<String, Object> transportHeaders;
    protected String fileURI;

    protected AbstractInjectHandler(String injectingSeq, String onErrorSeq, boolean sequential,
                                    SynapseEnvironment synapseEnvironment, VFSConfig vfsProperties) {
        this.injectingSeq = injectingSeq;
        this.onErrorSeq = onErrorSeq;
        this.sequential = sequential;
        this.synapseEnvironment = synapseEnvironment;
        this.vfsProperties = vfsProperties;
    }

    /**
     * Create the initial Synapse message context for a file/chunk/record, seeded with the current
     * transport headers.
     */
    protected org.apache.synapse.MessageContext createMessageContext() {
        org.apache.synapse.MessageContext msgCtx = synapseEnvironment.createMessageContext();
        MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(UUIDGenerator.getUUID());
        // Each message gets its OWN copy of the transport headers. The map is mutated during
        // mediation (e.g. ERROR_CODE is written on failure), so sharing one instance across the
        // many message contexts of a streamed file would leak one record's error onto every
        // subsequent record and mark them all as failures.
        axis2MsgCtx.setProperty(TRANSPORT_HEADERS,
                transportHeaders != null ? new HashMap<>(transportHeaders) : new HashMap<>());
        msgCtx.setProperty(MessageContext.CLIENT_API_NON_BLOCKING, true);
        return msgCtx;
    }

    /**
     * Select the Axis2 message builder for the given content type, falling back to SOAP.
     */
    protected Builder resolveBuilder(String contentType, MessageContext axis2MsgCtx)
            throws AxisFault {
        if (contentType == null) {
            return new SOAPBuilder();
        }
        int index = contentType.indexOf(';');
        String type = index > 0 ? contentType.substring(0, index) : contentType;
        Builder builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
        if (builder == null) {
            builder = new SOAPBuilder();
        }
        return builder;
    }

    /**
     * Inject a prepared message context into the configured sequence and report whether it
     * completed without an error code.
     *
     * @param name        the inbound endpoint name (for logging context)
     * @param msgCtx      the Synapse message context to inject
     * @param axis2MsgCtx its Axis2 counterpart
     * @return true if the injection completed without an error code being set
     */
    protected boolean injectToSequence(String name, org.apache.synapse.MessageContext msgCtx,
                                       MessageContext axis2MsgCtx) {
        if (injectingSeq == null || injectingSeq.equals("")) {
            log.error("Sequence name not specified. Sequence : " + injectingSeq);
            return false;
        }
        SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration()
                .getSequence(injectingSeq);
        if (seq == null) {
            log.error("Sequence: " + injectingSeq + " not found");
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("injecting message to sequence : " + injectingSeq);
        }
        if (!seq.isInitialized()) {
            seq.init(synapseEnvironment);
        }
        seq.setErrorHandler(onErrorSeq);

        // APPEND MODE: ensure any VFS write/Respond appends instead of overwriting by setting the
        // Axis2 property the VFS sender/transport checks.
        if (vfsProperties.isAppend()) {
            axis2MsgCtx.setProperty("transport.vfs.Append", "true");
        }

        synapseEnvironment.injectInbound(msgCtx, seq, sequential);

        @SuppressWarnings("unchecked")
        Map<String, Object> responseHeaders =
                (Map<String, Object>) axis2MsgCtx.getProperty(TRANSPORT_HEADERS);
        String errorCode = (responseHeaders != null)
                ? (String) responseHeaders.get(VFSConstants.ERROR_CODE) : null;
        return StringUtils.isEmpty(errorCode);
    }

    /**
     * Inject a prepared message context into a sequence named at runtime, rather than the
     * inbound's configured injecting sequence. Used for the once-per-file completion callbacks,
     * which are a different sequence from the one each record goes to.
     * <p>
     * Unlike {@link #injectToSequence}, this always mediates inline ({@code sequential = true}) so
     * the callback has finished before the file is post-processed, and it reports a missing
     * sequence rather than treating it as a mediation failure of the file.
     *
     * @param seqName the sequence to invoke
     * @return true if the sequence ran without setting an error code
     */
    protected boolean injectToNamedSequence(String seqName,
                                            org.apache.synapse.MessageContext msgCtx,
                                            MessageContext axis2MsgCtx) {
        SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration()
                .getSequence(seqName);
        if (seq == null) {
            log.error("Sequence '" + seqName + "' not found; skipping the callback.");
            return false;
        }
        if (!seq.isInitialized()) {
            seq.init(synapseEnvironment);
        }
        seq.setErrorHandler(onErrorSeq);
        synapseEnvironment.injectInbound(msgCtx, seq, true);

        @SuppressWarnings("unchecked")
        Map<String, Object> responseHeaders =
                (Map<String, Object>) axis2MsgCtx.getProperty(TRANSPORT_HEADERS);
        String errorCode = (responseHeaders != null)
                ? (String) responseHeaders.get(VFSConstants.ERROR_CODE) : null;
        return StringUtils.isEmpty(errorCode);
    }

    protected void seedInboundProperties(org.apache.synapse.MessageContext msgCtx, String name) {
        msgCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, name);
        msgCtx.setProperty(SynapseConstants.ARTIFACT_NAME,
                SynapseConstants.FAIL_SAFE_MODE_INBOUND_ENDPOINT + name);
        msgCtx.setProperty(SynapseConstants.IS_INBOUND, true);
    }

    /**
     * @param transportHeaders the transportHeaders to set
     */
    public void setTransportHeaders(Map<String, Object> transportHeaders) {
        this.transportHeaders = transportHeaders;
    }

    public void setFileURI(String fileURI) {
        this.fileURI = fileURI;
    }
}
