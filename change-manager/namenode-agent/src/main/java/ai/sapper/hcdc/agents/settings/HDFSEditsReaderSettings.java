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
