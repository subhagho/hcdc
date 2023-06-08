package ai.sapper.hcdc.agents.settings;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.messaging.MessagingProcessorSettings;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageSenderSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ChangeDeltaProcessorSettings extends MessagingProcessorSettings {
    public static final String __CONFIG_PATH = "processor.source";
    public static final String __CONFIG_PATH_SENDER = "sender";

    public static class Constants {
        public static final String CONFIG_RECEIVE_TIMEOUT = "readBatchTimeout";
        public static final String CONFIG_SENDER_BUILDER_TYPE = "sender.builder.type";
        public static final String CONFIG_SENDER_BUILDER_SETTINGS_TYPE = "sender.builder.settingsType";
    }

    @Config(name = Constants.CONFIG_SENDER_BUILDER_TYPE, type = Class.class)
    private Class<? extends MessageSenderBuilder<?, ?>> sendBuilderType;
    @Config(name = Constants.CONFIG_SENDER_BUILDER_SETTINGS_TYPE, type = Class.class)
    private Class<? extends MessageSenderSettings> sendBuilderSettingsType;
    @Config(name = Constants.CONFIG_RECEIVE_TIMEOUT, required = false, type = Long.class)
    private long receiveBatchTimeout = 1000;
}
