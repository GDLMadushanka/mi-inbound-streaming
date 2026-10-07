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

import java.io.InputStream;
import java.util.Iterator;

public interface StreamingProcessor {

    /**
     * Get an iterator of chunks (batches of records) from the input stream.
     * Use this for batch processing (multiple rows per chunk).
     * <p>
     * Guarantees:
     * - Each StreamChunk contains multiple complete, valid records
     * - No partial records or split fields
     * - Multi-byte characters are never split
     * - Record boundaries are preserved
     * <p>
     * Structure:
     * - Headers stored on StreamChunk (always)
     * - Individual StreamRecords in chunk (no headers)
     * - Metadata on StreamRecords (if configured)
     *
     * @param input the input stream to read from
     * @param contentType the MIME type
     * @param chunkSize records per chunk
     * @param startFromRecord number of leading records to skip (checkpoint resume); 0 = start from
     *                        the beginning. The first emitted record is {@code startFromRecord + 1}.
     * @param startFromChunk  chunks already emitted before the resume, so chunk numbering carries
     *                        on instead of restarting at 1. The first emitted chunk is
     *                        {@code startFromChunk + 1}.
     * @return an Iterator of StreamChunk (batches of records)
     * @throws StreamingException if initialization fails
     */
    Iterator<StreamChunk> getChunkIterator(InputStream input, String contentType, int chunkSize,
        long startFromRecord, long startFromChunk) throws StreamingException;

    /** Convenience overload that starts from the beginning (nothing skipped). */
    default Iterator<StreamChunk> getChunkIterator(InputStream input, String contentType,
        int chunkSize) throws StreamingException {
        return getChunkIterator(input, contentType, chunkSize, 0L, 0L);
    }

    /**
     * Get an iterator of individual records from the input stream.
     * Use this for row-by-row processing (one record per iteration).
     * <p>
     * Guarantees:
     * - Each StreamRecord is a complete, valid record
     * - No partial records or split fields
     * - Multi-byte characters are never split
     * - Record boundaries are preserved
     * <p>
     * Structure:
     * - Headers on StreamRecord (if configured)
     * - Metadata on StreamRecord (if configured)
     *
     * @param input the input stream to read from
     * @param contentType the MIME type
     * @param startFromRecord number of leading records to skip (checkpoint resume); 0 = start from
     *                        the beginning. The first emitted record is {@code startFromRecord + 1}.
     * @return an Iterator of StreamRecord (individual records)
     * @throws StreamingException if initialization fails
     */
    Iterator<StreamRecord> getRecordIterator(InputStream input, String contentType,
        long startFromRecord) throws StreamingException;

    /** Convenience overload that starts from the beginning (no records skipped). */
    default Iterator<StreamRecord> getRecordIterator(InputStream input, String contentType)
        throws StreamingException {
        return getRecordIterator(input, contentType, 0L);
    }

    /**
     * Serialize the valid records of a chunk into a single raw message body, in this format's
     * native representation (e.g. newline-joined lines for text/CSV, a JSON array for JSON/JSONL).
     * <p>
     * Used only in raw-content chunk mode (i.e. when the output is not routed to a variable).
     * Invalid records are excluded - they are siphoned to the failed-records file separately.
     *
     * @param chunk the chunk whose valid records should be serialized
     * @return the chunk body bytes, encoded with the chunk's charset
     */
    byte[] buildChunkBody(StreamChunk chunk);

}
