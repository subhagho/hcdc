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

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class NameNodeStatus {
    public static class Contants {
        public static final String KEY_HEADER = "name";
        public static final String KEY_M_TYPE = "modelerType";
        public static final String KEY_STATE = "State";
        public static final String KEY_HOST = "HostAndPort";
        public static final String KEY_ROLE = "NNRole";
        public static final String KEY_SEC_ENABLED = "SecurityEnabled";
        public static final String KEY_HA_TRANSITION_TIME = "LastHATransitionTime";
    }

    private String header;
    private String modelerType;
    private String state;
    private String nNRole;
    private String host;
    private boolean securityEnabled;
    private long haLastTransitionTime;

    public NameNodeStatus parse(@NonNull Map<String, String> map) {
        header = map.get(Contants.KEY_HEADER);
        modelerType = map.get(Contants.KEY_M_TYPE);
        state = map.get(Contants.KEY_STATE);
        nNRole = map.get(Contants.KEY_ROLE);
        host = map.get(Contants.KEY_HOST);
        String s = map.get(Contants.KEY_SEC_ENABLED);
        if (!Strings.isNullOrEmpty(s)) {
            securityEnabled = Boolean.parseBoolean(s);
        }
        s = map.get(Contants.KEY_HA_TRANSITION_TIME);
        if (!Strings.isNullOrEmpty(s)) {
            haLastTransitionTime = Long.parseLong(s);
        }
        return this;
    }
}
