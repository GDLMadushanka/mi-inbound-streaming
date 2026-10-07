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

import org.junit.Assert;
import org.junit.Test;
import org.wso2.carbon.inbound.streaming.StreamingConstants;

import java.util.Properties;

/**
 * Verifies how {@link VFSConfig} resolves the streaming charset into the derived streaming
 * content type.
 */
public class VFSConfigStreamingCharsetTest {

    private Properties streamingProps(String format, String charset) {
        Properties p = new Properties();
        p.setProperty(StreamingConstants.STREAMING, "true");
        p.setProperty(StreamingConstants.STREAMING_MODE, StreamingConstants.STREAMING_MODE_RECORD);
        p.setProperty(StreamingConstants.STREAMING_INPUT_FORMAT, format);
        if (charset != null) {
            p.setProperty(StreamingConstants.STREAMING_CHARSET, charset);
        }
        return p;
    }

    @Test
    public void testDefaultCharsetFoldedIntoContentType() {
        VFSConfig config = new VFSConfig(streamingProps(StreamingConstants.STREAMING_FORMAT_JSON, null));
        Assert.assertEquals("UTF-8", config.getStreamingCharset());
        Assert.assertEquals("application/json; charset=UTF-8", config.getStreamingContentType());
    }

    @Test
    public void testCustomCharsetFoldedIntoContentType() {
        VFSConfig config = new VFSConfig(streamingProps(StreamingConstants.STREAMING_FORMAT_CSV, "UTF-16"));
        Assert.assertEquals("UTF-16", config.getStreamingCharset());
        Assert.assertEquals("text/csv; charset=UTF-16", config.getStreamingContentType());
    }

    @Test
    public void testIso88591IsSupported() {
        VFSConfig config = new VFSConfig(streamingProps(StreamingConstants.STREAMING_FORMAT_TEXT, "ISO-8859-1"));
        Assert.assertEquals("text/plain; charset=ISO-8859-1", config.getStreamingContentType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCharsetFailsFast() {
        new VFSConfig(streamingProps(StreamingConstants.STREAMING_FORMAT_JSON, "NOT-A-CHARSET-123"));
    }

    @Test
    public void testEntireFileModeDoesNotFoldCharset() {
        Properties p = streamingProps(StreamingConstants.STREAMING_FORMAT_JSON, "UTF-16");
        p.setProperty(StreamingConstants.STREAMING_MODE, StreamingConstants.STREAMING_MODE_ENTIRE_FILE);
        VFSConfig config = new VFSConfig(p);
        // ENTIRE_FILE does not stream-parse, so no charset is folded into the content type.
        Assert.assertEquals("application/json", config.getStreamingContentType());
    }

    @Test
    public void testStreamingDisabledDoesNotFoldCharset() {
        Properties p = new Properties();
        p.setProperty(StreamingConstants.STREAMING, "false");
        p.setProperty(StreamingConstants.STREAMING_INPUT_FORMAT, StreamingConstants.STREAMING_FORMAT_JSON);
        p.setProperty(StreamingConstants.STREAMING_CHARSET, "UTF-16");
        VFSConfig config = new VFSConfig(p);
        Assert.assertEquals("application/json", config.getStreamingContentType());
    }
}
