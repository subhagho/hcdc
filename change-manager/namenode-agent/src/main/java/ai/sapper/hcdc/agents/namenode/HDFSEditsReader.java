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

package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.model.EHCdcProcessorState;
import ai.sapper.cdc.core.model.HCdcProcessingState;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.processing.Processor;
import ai.sapper.hcdc.agents.settings.HDFSEditsReaderSettings;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public abstract class HDFSEditsReader extends Processor<EHCdcProcessorState, HCdcTxId> {
    protected MessageSender<String, DFSChangeDelta> sender;
    protected HDFSEditsReaderSettings settings;

    public HDFSEditsReader(@NonNull NameNodeEnv env) {
        super(env, HCdcProcessingState.class);
    }

    @SuppressWarnings("unchecked")
    public void setup(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                      @NonNull Class<? extends HDFSEditsReaderSettings> settingsType,
                      @NonNull BaseEnv<?> env) throws Exception {
        ConfigReader reader = new ConfigReader(xmlConfig, settingsType);
        reader.read();
        settings = (HDFSEditsReaderSettings) reader.settings();
        super.init(settings);

        MessageSenderBuilder<String, DFSChangeDelta> builder
                = (MessageSenderBuilder<String, DFSChangeDelta>) settings.getBuilderType()
                .getDeclaredConstructor()
                .newInstance();
        HierarchicalConfiguration<ImmutableNode> eConfig
                = reader.config().configurationAt(HDFSEditsReaderSettings.__CONFIG_PATH_SENDER);
        if (eConfig == null) {
            throw new ConfigurationException(
                    String.format("Sender queue configuration not found. [path=%s]",
                            HDFSEditsReaderSettings.__CONFIG_PATH_SENDER));
        }
        sender = builder.withEnv(env).build(eConfig);
    }
}
