package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.core.messaging.MessageReceiver;
import ai.sapper.hcdc.core.messaging.MessageSender;
import ai.sapper.hcdc.core.messaging.MessagingConfig;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class HDFSDeltaChangeManager {
    private final ZkStateManager stateManager;
    private MessageSender<String, DFSChangeDelta> sender;
    private MessageReceiver<String, DFSChangeDelta> receiver;

    public HDFSDeltaChangeManager(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class HDFSDeltaChangeConfig extends ConfigReader {
        public static class Constants {
            public static final String __CONFIG_PATH = "delta.manager";
            public static final String __CONFIG_PATH_SENDER = "sender";
            public static final String __CONFIG_PATH_RECEIVER = "receiver";
        }

        private MessagingConfig senderConfig;
        private MessagingConfig receiverConfig;

        public HDFSDeltaChangeConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, path);
        }
    }
}
