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

package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.config.Config;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *      <state>
 *          <stateManagerClass>[State Manager class]</stateManagerClass>
 *          <connection>[ZooKeeper connection name]</connection>
 *          <locking> -- Optional
 *              <retry>[Lock retry count, default = 4]</retry>
 *              <timeout>[Lock timeout, default = 15sec</timeout>
 *          </locking>
 *          <offsets>
 *              <offsetManager>
 *                  ...
 *              </offsetManager>
 *              ...
 *          </offsets>
 *          <fileState>[true|false, default = false</fileState>
 *      </state>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class HCdcStateManagerSettings extends BaseStateManagerSettings {
    @Config(name = "fileState", required = false, type = Boolean.class)
    private boolean requireFileState = false;
}
