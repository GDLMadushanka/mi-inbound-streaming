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

package org.wso2.carbon.inbound.streaming.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;

/**
 * Handler to manage post processing actions
 */
public class PostProcessingHandler implements PostProcessor {

    private Log log = LogFactory.getLog(PostProcessingHandler.class);
    private Action onSuccessAction;
    private Action onFailAction;

    @Override
    public void onSuccess(FileObject fileObject) throws Exception {

        if (onSuccessAction != null) {
            onSuccessAction.execute(fileObject);
        }
    }

    @Override
    public void onFail(FileObject fileObject) throws Exception {
        if (onFailAction != null) {
            onFailAction.execute(fileObject);
        }
    }

    public void setOnSuccessAction(Action onSuccessAction) {
        this.onSuccessAction = onSuccessAction;
    }

    public void setOnFailAction(Action onFailAction) {
        this.onFailAction = onFailAction;
    }
}
