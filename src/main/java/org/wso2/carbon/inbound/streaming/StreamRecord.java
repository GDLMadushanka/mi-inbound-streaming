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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Represents a single record in the streaming datasource. If CSV, this would represent a single row.
 */
public class StreamRecord {

    private long recordNumber;
    private byte[] content;
    /**
     * Data to be added in the output property.
     */
    private JsonElement JSONPayload;
    private boolean isValid = true;
    private String parseError;
    private Charset encoding = StandardCharsets.UTF_8;

    public StreamRecord(long recordNumber) {
        this.recordNumber = recordNumber;
    }

    public long getRecordNumber() {
        return recordNumber;
    }

    public void setRecordNumber(long recordNumber) {
        this.recordNumber = recordNumber;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public JsonElement getJSONPayload() {
        return JSONPayload;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public String getParseError() {
        return parseError;
    }

    public void setParseError(String parseError) {
        this.parseError = parseError;
    }

    public Charset getEncoding() {
        return encoding;
    }

    public void setEncoding(Charset encoding) {
        this.encoding = encoding != null ? encoding : StandardCharsets.UTF_8;
    }

    public String getAsString() {
        return new String(content, encoding);
    }

    public void setJSONPayload(JsonElement payload) {
        this.JSONPayload = payload;
    }

    @Override
    public String toString() {
        return "StreamRecord{" +
                "rowNumber=" + recordNumber +
                ", isValid=" + isValid +
                '}';
    }
}
