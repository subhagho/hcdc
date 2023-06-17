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

package ai.sapper.hcdc.agents.settings;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageSenderSettings;
import ai.sapper.cdc.core.processing.ProcessorSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class HDFSEditsReaderSettings extends ProcessorSettings {
    public static final String __CONFIG_PATH = "processor.edits";
    public static final String __CONFIG_PATH_SENDER = "sender";
    public static class Constants {
        public static final String CONFIG_SENDER_BUILDER_TYPE = "sender.builder.type";
        public static final String CONFIG_SENDER_BUILDER_SETTINGS_TYPE = "sender.builder.settingsType";
        public static final String CONFIG_POLL_INTERVAL = "pollingInterval";
    }

    @Config(name = Constants.CONFIG_SENDER_BUILDER_TYPE, type = Class.class)
    private Class<? extends MessageSenderBuilder<?, ?>> builderType;
    @Config(name = Constants.CONFIG_SENDER_BUILDER_SETTINGS_TYPE, type = Class.class)
    private Class<? extends MessageSenderSettings> builderSettingsType;
    @Config(name = Constants.CONFIG_POLL_INTERVAL, required = false, type = Long.class)
    private long pollingInterval = 60000; // By default, run every minute
}
