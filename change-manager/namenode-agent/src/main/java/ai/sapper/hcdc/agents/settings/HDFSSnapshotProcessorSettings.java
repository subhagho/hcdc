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
public class HDFSSnapshotProcessorSettings extends ProcessorSettings {
    public static final String __CONFIG_PATH = "processor.snapshot";
    public static final String __CONFIG_PATH_SENDER = "sender";
    public static final String __CONFIG_PATH_ADMIN = "admin";

    public static final class Constants {
        public static final String CONFIG_SENDER_BUILDER_TYPE = "sender.builder.type";
        public static final String CONFIG_SENDER_BUILDER_SETTINGS_TYPE = "sender.builder.settingsType";
        public static final String CONFIG_ADMIN_SENDER_BUILDER_TYPE = "admin.builder.type";
        public static final String CONFIG_ADMIN_SENDER_BUILDER_SETTINGS_TYPE = "admin.builder.settingsType";
        private static final String CONFIG_EXECUTOR_POOL_SIZE = "executorPoolSize";
    }

    @Config(name = Constants.CONFIG_SENDER_BUILDER_TYPE, type = Class.class)
    private Class<? extends MessageSenderBuilder<?, ?>> builderType;
    @Config(name = Constants.CONFIG_SENDER_BUILDER_SETTINGS_TYPE, type = Class.class)
    private Class<? extends MessageSenderSettings> builderSettingsType;
    @Config(name = Constants.CONFIG_ADMIN_SENDER_BUILDER_TYPE, type = Class.class)
    private Class<? extends MessageSenderBuilder<?, ?>> adminBuilderType;
    @Config(name = Constants.CONFIG_ADMIN_SENDER_BUILDER_SETTINGS_TYPE, type = Class.class)
    private Class<? extends MessageSenderSettings> adminBuilderSettingsType;
    @Config(name = Constants.CONFIG_EXECUTOR_POOL_SIZE, required = false, type = Integer.class)
    private int executorPoolSize = 4;
}