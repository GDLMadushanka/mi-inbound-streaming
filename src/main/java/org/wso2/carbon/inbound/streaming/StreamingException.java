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

/**
 * Signals a failure raised while a stream is being set up or consumed.
 * <p>
 * This is an <em>unchecked</em> exception on purpose: streaming content is handed out through
 * {@link java.util.Iterator}s whose {@code hasNext()}/{@code next()} methods cannot declare checked
 * exceptions, so a failure discovered mid-iteration (e.g. malformed JSON halfway through a file)
 * still needs to be able to propagate out to the consumer.
 * <p>
 * The {@link #isRecoverable() recoverable} flag distinguishes two very different failures:
 * <ul>
 *     <li><b>Not recoverable</b> - the input itself is broken (e.g. malformed JSON). The remaining
 *     bytes can no longer be interpreted, so there is no point continuing; the file must be handed
 *     to the configured <em>action after failure</em> (move / delete).</li>
 *     <li><b>Recoverable</b> - the record parsed fine but something downstream (e.g. mediation of a
 *     single record) failed. The stream is still structurally intact and processing could, in
 *     principle, continue with subsequent records.</li>
 * </ul>
 */
public class StreamingException extends RuntimeException {

    /**
     * The nature of the failure, used to classify a non-recoverable abort so it can be reported
     * accurately (e.g. count a fatal parse error against the parse-failure metric).
     * <ul>
     *     <li>{@link #PARSE} - the bytes could not be parsed as the configured format (malformed
     *     JSON document, broken CSV quoting, ...). The offending record is a parse failure.</li>
     *     <li>{@link #IO} - an underlying read failure; not attributable to a single record.</li>
     *     <li>{@link #OTHER} - anything else (e.g. setup/initialization).</li>
     * </ul>
     */
    public enum Kind { PARSE, IO, OTHER }

    private final long rowNumber;
    private final boolean isRecoverable;
    private final Kind kind;

    public StreamingException(String message) {
        this(message, null, -1, true, Kind.OTHER);
    }

    public StreamingException(String message, Throwable cause) {
        this(message, cause, -1, true, Kind.OTHER);
    }

    public StreamingException(String message, Throwable cause, long rowNumber, boolean isRecoverable) {
        this(message, cause, rowNumber, isRecoverable, Kind.OTHER);
    }

    public StreamingException(String message, Throwable cause, long rowNumber, boolean isRecoverable,
                              Kind kind) {
        super(message, cause);
        this.rowNumber = rowNumber;
        this.isRecoverable = isRecoverable;
        this.kind = kind != null ? kind : Kind.OTHER;
    }

    public long getRowNumber() {
        return rowNumber;
    }

    public boolean isRecoverable() {
        return isRecoverable;
    }

    /** @return the classification of this failure (parse / io / other). */
    public Kind getKind() {
        return kind;
    }

    @Override
    public String toString() {
        return "StreamingException{" +
                "message='" + getMessage() + '\'' +
                ", rowNumber=" + rowNumber +
                ", isRecoverable=" + isRecoverable +
                '}';
    }
}
