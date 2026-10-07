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

import org.wso2.carbon.inbound.streaming.VFSConfig;
import org.wso2.carbon.inbound.streaming.csv.CSVStreamingProcessor;
import org.wso2.carbon.inbound.streaming.json.JSONStreamingProcessor;
import org.wso2.carbon.inbound.streaming.jsonl.JSONLStreamingProcessor;
import org.wso2.carbon.inbound.streaming.text.TextStreamingProcessor;

/**
 * Selects a {@link StreamingProcessor} based on the configured streaming input format.
 * <p>
 * TEXT, CSV, JSON and JSONL are implemented today. The selection is format-driven so that other
 * processors (e.g. XML) can be registered here later without touching callers.
 */
public class StreamingProcessorFactory {

    private StreamingProcessorFactory() {
    }

    /**
     * Return a processor for the given streaming input format, configured from the supplied
     * VFS configuration, or {@code null} if no processor is registered for that format.
     *
     * @param format the streaming input format (e.g. text, csv, json, xml)
     * @param config the inbound VFS configuration carrying streaming parameters
     * @return a matching {@link StreamingProcessor}, or {@code null} if unsupported
     */
    public static StreamingProcessor getProcessor(String format, VFSConfig config) {
        if (format == null) {
            return null;
        }

        switch (format.toLowerCase()) {
            case StreamingConstants.STREAMING_FORMAT_CSV:
                return new CSVStreamingProcessor(
                        config.getStreamingBufferSize(),
                        config.getStreamingCsvDelimiter(),
                        config.getStreamingCsvQuote(),
                        config.isStreamingCsvHasHeader(),
                        config.isStreamingAddOutputToProperty(),
                        config.isStreamingAddHeadersToEachResult(),
                        config.getStreamingCsvDataTypes());
            case StreamingConstants.STREAMING_FORMAT_TEXT:
                return new TextStreamingProcessor(
                        config.getStreamingBufferSize(),
                        config.isStreamingAddOutputToProperty());
            case StreamingConstants.STREAMING_FORMAT_JSON:
                return new JSONStreamingProcessor(
                        config.getStreamingBufferSize(),
                        config.getStreamingJsonPath(),
                        config.isStreamingAddOutputToProperty());
            case StreamingConstants.STREAMING_FORMAT_JSONL:
                return new JSONLStreamingProcessor(
                        config.getStreamingBufferSize(),
                        config.isStreamingAddOutputToProperty());
            // XML processor is not implemented yet.
            default:
                return null;
        }
    }
}
