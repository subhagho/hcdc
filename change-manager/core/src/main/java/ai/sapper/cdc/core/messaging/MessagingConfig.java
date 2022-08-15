package ai.sapper.cdc.core.messaging;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public class MessagingConfig {
    public static class Constants {
        public static final String CONFIG_CONNECTION_TYPE = "connectionType";
        public static final String CONFIG_CONNECTION = "connection";
        public static final String CONFIG_PARTITIONER_CLASS = "partitioner.type";
        public static final String CONFIG_BATCH_SIZE = "batchSize";
    }
    private HierarchicalConfiguration<ImmutableNode> config;

    private String type;
    private String connection;
    private String partitionerClass;
    private int batchSize = -1;

    public void read(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        type = config.getString(Constants.CONFIG_CONNECTION_TYPE);
        if (Strings.isNullOrEmpty(type)) {
            throw new ConfigurationException(String.format("Snapshot Manager Manager Configuration Error: missing [%s]", Constants.CONFIG_CONNECTION_TYPE));
        }
        connection = config.getString(Constants.CONFIG_CONNECTION);
        if (Strings.isNullOrEmpty(connection)) {
            throw new ConfigurationException(String.format("Snapshot Manager Manager Configuration Error: missing [%s]", Constants.CONFIG_CONNECTION));
        }
        if (config.containsKey(Constants.CONFIG_PARTITIONER_CLASS))
            partitionerClass = config.getString(Constants.CONFIG_PARTITIONER_CLASS);
        if (config.containsKey(Constants.CONFIG_BATCH_SIZE)) {
            String s = config.getString(Constants.CONFIG_BATCH_SIZE);
            batchSize = Integer.parseInt(s);
        }
        this.config = config;
    }
}
