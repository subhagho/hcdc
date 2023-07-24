/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.cdc.core.model;

import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.core.processing.MessageTaskResponse;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class HCdcTaskResponse extends MessageTaskResponse<HCdcTxId, String, DFSChangeDelta> {
    public static final String CONTEXT_KEY_DATA = "data";

    public HCdcTaskResponse data(@NonNull Object data) {
        if (context() == null) {
            context(new Context());
        }
        context().put(CONTEXT_KEY_DATA, data);
        return this;
    }

    public Object data() {
        if (context() != null) {
            return context().get(CONTEXT_KEY_DATA);
        }
        return null;
    }
}
