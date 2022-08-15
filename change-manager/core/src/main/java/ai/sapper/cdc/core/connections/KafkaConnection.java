package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.ConfigReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public abstract class KafkaConnection implements MessageConnection {
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();

    private KafkaConfig kafkaConfig;

    /**
     * @return
     */
    @Override
    public String name() {
        Preconditions.checkNotNull(kafkaConfig);
        return kafkaConfig.name;
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        try {
            if (state.isConnected()) {
                close();
            }
            state.clear(EConnectionState.Unknown);
            kafkaConfig = new KafkaConfig(xmlConfig);
            kafkaConfig.read();

        } catch (Throwable t) {
            state.error(t);
            throw new ConnectionError("Error opening HDFS connection.", t);
        }
        return this;
    }

    /**
     * @return
     */
    @Override
    public Throwable error() {
        return state.error();
    }

    /**
     * @return
     */
    @Override
    public EConnectionState connectionState() {
        return state.state();
    }

    /**
     * @return
     */
    @Override
    public boolean isConnected() {
        return (state.isConnected());
    }

    /**
     * @return
     */
    @Override
    public HierarchicalConfiguration<ImmutableNode> config() {
        return kafkaConfig.config();
    }

    public String topic() {
        Preconditions.checkState(kafkaConfig != null);
        return kafkaConfig.topic;
    }

    enum EKafkaClientMode {
        Producer, Consumer
    }

    @Getter
    @Accessors(fluent = true)
    public static class KafkaConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "kafka";

        private static class Constants {
            private static final String CONFIG_NAME = "name";
            private static final String CONFIG_MODE = "mode";
            public static final String CONFIG_FILE_CONFIG = "config";
            public static final String CONFIG_PRODUCER_CONFIG = String.format("producer.%s", CONFIG_FILE_CONFIG);
            public static final String CONFIG_CONSUMER = "consumer";
            public static final String CONFIG_PARTITIONS = "partitions";
            public static final String CONFIG_TOPIC = "topic";
        }


        private String name;
        private String configPath;
        private Properties properties;
        private EKafkaClientMode mode = EKafkaClientMode.Producer;
        private String topic;

        private List<Integer> partitions;
        private Map<String, String> parameters;

        public KafkaConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not set or is NULL");
            }
            try {
                name = get().getString(Constants.CONFIG_NAME);
                if (Strings.isNullOrEmpty(name)) {
                    throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]",
                            Constants.CONFIG_NAME));
                }
                String s = get().getString(Constants.CONFIG_MODE);
                if (!Strings.isNullOrEmpty(name)) {
                    mode = EKafkaClientMode.valueOf(s);
                }

                if (mode == EKafkaClientMode.Producer) {
                    configPath = get().getString(Constants.CONFIG_PRODUCER_CONFIG);
                    if (Strings.isNullOrEmpty(configPath)) {
                        throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]",
                                Constants.CONFIG_PRODUCER_CONFIG));
                    }
                    File cf = new File(configPath);
                    if (!cf.exists()) {
                        throw new ConfigurationException(String.format("Invalid Producer configuration file. [path=%s]", cf.getAbsolutePath()));
                    }
                    properties = new Properties();
                    properties.load(new FileInputStream(cf));
                } else if (mode == EKafkaClientMode.Consumer) {
                    HierarchicalConfiguration<ImmutableNode> cnode = get().configurationAt(Constants.CONFIG_CONSUMER);
                    if (cnode == null) {
                        throw new ConfigurationException(
                                String.format("Invalid Consumer configuration: missing path. [path=%s]",
                                        Constants.CONFIG_CONSUMER));
                    }
                    configPath = cnode.getString(Constants.CONFIG_FILE_CONFIG);
                    if (Strings.isNullOrEmpty(configPath)) {
                        throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]",
                                Constants.CONFIG_FILE_CONFIG));
                    }
                    File cf = new File(configPath);
                    if (!cf.exists()) {
                        throw new ConfigurationException(String.format("Invalid Consumer configuration file. [path=%s]", cf.getAbsolutePath()));
                    }
                    properties = new Properties();
                    properties.load(new FileInputStream(cf));

                    String ps = null;
                    partitions = new ArrayList<>();
                    if (cnode.containsKey(Constants.CONFIG_PARTITIONS)) {
                        ps = cnode.getString(Constants.CONFIG_PARTITIONS);
                    }
                    if (!Strings.isNullOrEmpty(ps)) {
                        String[] parts = ps.split(";");
                        for (String part : parts) {
                            Integer p = Integer.parseInt(part);
                            partitions.add(p);
                        }
                    } else {
                        partitions.add(0);
                    }
                }

                topic = get().getString(Constants.CONFIG_TOPIC);
                if (Strings.isNullOrEmpty(topic)) {
                    throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]", Constants.CONFIG_TOPIC));
                }

                parameters = readParameters();
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing Kafka configuration.", t);
            }
        }
    }
}
