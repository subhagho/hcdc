package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.processing.Processor;
import ai.sapper.cdc.core.model.HCdcProcessingState;
import ai.sapper.cdc.core.HCdcStateManager;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.model.EHCdcProcessorState;
import ai.sapper.cdc.core.model.HCdcTxId;
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
    protected final NameNodeEnv env;
    protected MessageSender<String, DFSChangeDelta> sender;
    protected HDFSEditsReaderSettings settings;

    public HDFSEditsReader(@NonNull NameNodeEnv env,
                           @NonNull HCdcStateManager stateManager) {
        super(stateManager, HCdcProcessingState.class);
        this.env = env;
    }

    @SuppressWarnings("unchecked")
    public void setup(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                      @NonNull Class<? extends HDFSEditsReaderSettings> settingsType) throws Exception {
        ConfigReader reader = new ConfigReader(xmlConfig, settingsType);
        reader.read();
        settings = (HDFSEditsReaderSettings) reader.settings();
        super.init(settings);

        MessageSenderBuilder<String, DFSChangeDelta> builder
                = (MessageSenderBuilder<String, DFSChangeDelta>) settings.getBuilderType()
                .getDeclaredConstructor(BaseEnv.class, Class.class)
                .newInstance(env, settings.getBuilderSettingsType());
        HierarchicalConfiguration<ImmutableNode> eConfig
                = reader.config().configurationAt(HDFSEditsReaderSettings.__CONFIG_PATH_SENDER);
        if (eConfig == null) {
            throw new ConfigurationException(
                    String.format("Sender queue configuration not found. [path=%s]",
                            HDFSEditsReaderSettings.__CONFIG_PATH_SENDER));
        }
        sender = builder.build(eConfig);
    }
}
