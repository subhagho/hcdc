package ai.sapper.cdc.core.connections.kafka;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.messaging.MessagingError;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public class KafkaAdminHelper {
    private AdminClient kafkaAdmin;
    private KafkaAdminConfig adminConfig;

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws MessagingError {
        try {
            adminConfig = new KafkaAdminConfig(config);
            adminConfig.read();

            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, adminConfig.bootstrapServers);

            kafkaAdmin = AdminClient.create(props);
        } catch (Exception e) {
            DefaultLogger.LOGGER.error(e.getLocalizedMessage());
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(e));
            throw new MessagingError(e);
        }
    }

    public void createTopic(@NonNull String name, int partitions, short replication, Map<String, String> topicConfig) throws MessagingError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkNotNull(kafkaAdmin);
        if (partitions <= 0) partitions = 1;
        if (replication <= 0) replication = 1;

        try {
            NewTopic topic = new NewTopic(name, partitions, replication);
            if (topicConfig != null) {
                topic.configs(topicConfig);
            }
            CreateTopicsResult result = kafkaAdmin.createTopics(
                    Collections.singleton(topic)
            );
            KafkaFuture<Void> future = result.values().get(name);
            future.get();

            DefaultLogger.LOGGER.info(String.format("Created new Kafka Topic. [name=%s]", name));
        } catch (Exception e) {
            throw new MessagingError(String.format("Error creating topic. [name=%s]", name), e);
        }
    }

    public void deleteTopic(@NonNull String name) throws MessagingError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkNotNull(kafkaAdmin);
        try {
            DeleteTopicsResult result = kafkaAdmin.deleteTopics(Collections.singleton(name));
            KafkaFuture<Void> future = result.topicNameValues().get(name);
            future.get();

            DefaultLogger.LOGGER.info(String.format("Created new Kafka Topic. [name=%s]", name));
        } catch (Exception e) {
            throw new MessagingError(String.format("Error deleting topic. [name=%s]", name), e);
        }
    }

    public static class KafkaAdminConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "kafka.admin";
        private static final String CONFIG_BOOTSTRAP = "servers";

        private String bootstrapServers;

        public KafkaAdminConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            bootstrapServers = config().getString(CONFIG_BOOTSTRAP);
            if (Strings.isNullOrEmpty(bootstrapServers)) {
                throw new ConfigurationException(String.format("Kafka Admin Configuration Error: missing [%s]", CONFIG_BOOTSTRAP));
            }
        }
    }
}
