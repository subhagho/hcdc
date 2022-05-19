package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
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
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public abstract class KafkaConnection implements Connection {
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

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {

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
            public static final String CONFIG_PRODUCER_CONFIG = "config.producer";
            public static final String CONFIG_CONSUMER_CONFIG = "config.consumer";
        }


        private String name;
        private String consumerConfig;
        private String producerConfig;
        private Properties consumerProperties;
        private Properties producerProperties;
        private EKafkaClientMode mode = EKafkaClientMode.Producer;

        private Map<String, String> parameters;

        public KafkaConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not drt or is NULL");
            }
            try {
                name = get().getString(Constants.CONFIG_NAME);
                if (Strings.isNullOrEmpty(name)) {
                    throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]", Constants.CONFIG_NAME));
                }
                String s = get().getString(Constants.CONFIG_MODE);
                if (!Strings.isNullOrEmpty(name)) {
                    mode = EKafkaClientMode.valueOf(s);
                }

                if (mode == EKafkaClientMode.Producer) {
                    producerConfig = get().getString(Constants.CONFIG_PRODUCER_CONFIG);
                    if (Strings.isNullOrEmpty(producerConfig)) {
                        throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]", Constants.CONFIG_PRODUCER_CONFIG));
                    }
                    File cf = new File(producerConfig);
                    if (!cf.exists()) {
                        throw new ConfigurationException(String.format("Invalid Producer configuration file. [path=%s]", cf.getAbsolutePath()));
                    }
                    producerProperties = new Properties();
                    producerProperties.load(new FileInputStream(cf));
                } else if (mode == EKafkaClientMode.Consumer) {
                    consumerConfig = get().getString(Constants.CONFIG_CONSUMER_CONFIG);
                    if (Strings.isNullOrEmpty(consumerConfig)) {
                        throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]", Constants.CONFIG_CONSUMER_CONFIG));
                    }
                    File cf = new File(consumerConfig);
                    if (!cf.exists()) {
                        throw new ConfigurationException(String.format("Invalid Consumer configuration file. [path=%s]", cf.getAbsolutePath()));
                    }
                    consumerProperties = new Properties();
                    consumerProperties.load(new FileInputStream(cf));
                }

                parameters = readParameters();
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }
    }
}
