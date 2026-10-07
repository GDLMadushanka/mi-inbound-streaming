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

/**
 * Reply-file naming and the no-destination short circuit. The append paths themselves need a live
 * Axis2 message context, so they are exercised on the server rather than here.
 */
public class ReplyFileWriterTest {

    @Test
    public void testDefaultReplyFileNameIsJsonLinesForJsonPayloads() {
        Assert.assertEquals("response.jsonl", ReplyFileWriter.DEFAULT_JSON_REPLY_FILE);
    }

    @Test
    public void testDefaultReplyFileNameForNonJsonMatchesTheVfsTransportDefault() {
        Assert.assertEquals(VFSConstants.DEFAULT_RESPONSE_FILE,
            ReplyFileWriter.DEFAULT_XML_REPLY_FILE);
        Assert.assertEquals("response.xml", ReplyFileWriter.DEFAULT_XML_REPLY_FILE);
    }

    @Test
    public void testAppendIsANoOpForANullContext() {
        ReplyFileWriter writer = new ReplyFileWriter(null, null, "file:///tmp", null, "in.csv");
        writer.append(null);
        Assert.assertEquals(0, writer.getWrittenCount());
        writer.close();
    }

    @Test
    public void testAnUnopenableDestinationDoesNotThrow() {
        // No file system manager: opening must fail softly, leaving the writer a no-op rather than
        // failing the source file that has already been delivered to the sequence.
        ReplyFileWriter writer =
            new ReplyFileWriter(null, null, "file:///nonexistent-root/x", null, "in.csv");
        writer.append(null);
        Assert.assertEquals(0, writer.getWrittenCount());
        writer.close();
    }
}
