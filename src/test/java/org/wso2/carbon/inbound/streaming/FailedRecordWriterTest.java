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

public class FailedRecordWriterTest {

    @Test
    public void testParseErrorFileNaming() {
        // The 'parse.fail' marker is inserted before the last extension.
        Assert.assertEquals("input.parse.fail.jsonl",
                FailedRecordWriter.toFailedRecordsFileName("input.jsonl", "parse.fail"));
        Assert.assertEquals("data.parse.fail.csv",
                FailedRecordWriter.toFailedRecordsFileName("data.csv", "parse.fail"));
        // Multiple dots: only the last extension is preserved after the marker.
        Assert.assertEquals("archive.2026.parse.fail.json",
                FailedRecordWriter.toFailedRecordsFileName("archive.2026.json", "parse.fail"));
        // No extension: the marker is appended.
        Assert.assertEquals("records.parse.fail",
                FailedRecordWriter.toFailedRecordsFileName("records", "parse.fail"));
        // Leading-dot (hidden) file: treated as a name, not an extension.
        Assert.assertEquals(".gitignore.parse.fail",
                FailedRecordWriter.toFailedRecordsFileName(".gitignore", "parse.fail"));
    }

    @Test
    public void testMediationErrorFileNaming() {
        // The mediation marker yields a distinct sidecar so it never collides with the parse sidecar.
        Assert.assertEquals("input.mediation.fail.jsonl",
                FailedRecordWriter.toFailedRecordsFileName("input.jsonl", "mediation.fail"));
        Assert.assertEquals("data.mediation.fail",
                FailedRecordWriter.toFailedRecordsFileName("data", "mediation.fail"));
    }
}
