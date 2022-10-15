package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.kafka.KafkaAdminHelper;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class KafkaAdmin {
    private static class KafkaTopic {
        public static final String CONFIG_NAME = "name";
        public static final String CONFIG_REPLICAS = "replicas";
        public static final String CONFIG_MIN_ISR = "minIsr";
        public static final String CONFIG_PARTITIONS = "partitions";

        private String name;
        private int replicas = 1;
        private int minIsr = 1;
        private int partitions = 1;
    }

    public static final String NODE_TOPICS = "topics";
    public static final String NODE_TOPIC = String.format("%s.topic", NODE_TOPICS);
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--path", "-p"}, description = "Connection definition path.")
    private String configPath = null;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    @Parameter(names = {"--cmd", "-r"}, required = true, description = "Command to execute (c = create,d = delete, r = recreate)")
    private String cmd;

    private EConfigFileType fileSource = EConfigFileType.File;
    private HierarchicalConfiguration<ImmutableNode> config;
    private final KafkaAdminHelper helper = new KafkaAdminHelper();

    private void run() throws Exception {
        if (!Strings.isNullOrEmpty(configSource)) {
            fileSource = EConfigFileType.parse(configSource);
        }
        Preconditions.checkNotNull(fileSource);
        config = ConfigReader.read(configFile, fileSource);
        if (!Strings.isNullOrEmpty(configPath)) {
            config = config.configurationAt(configPath);
        }
        helper.init(config);

        List<KafkaTopic> topics = readTopics();

        if (cmd.compareToIgnoreCase("c") == 0) {
            runCreate(topics);
        } else if (cmd.compareToIgnoreCase("d") == 0) {
            runDelete(topics);
        } else if (cmd.compareToIgnoreCase("r") == 0) {
            runDelete(topics);
            runCreate(topics);
        } else {
            throw new Exception(String.format("Command not recognized. [cmd=%s]", cmd));
        }
    }

    private void runCreate(List<KafkaTopic> topics) throws Exception {
        if (topics != null && !topics.isEmpty()) {
            for (KafkaTopic topic : topics) {
                if (helper.exists(topic.name)) {
                    throw new Exception(String.format("Kafka Topic already exists. [name=%s]", topic.name));
                }
                helper.createTopic(topic.name,
                        topic.partitions,
                        (short) topic.replicas,
                        (short) topic.minIsr,
                        null);
            }
        }
    }

    private void runDelete(List<KafkaTopic> topics) throws Exception {
        if (topics != null && !topics.isEmpty()) {
            for (KafkaTopic topic : topics) {
                if (helper.exists(topic.name)) {
                    helper.deleteTopic(topic.name);
                }
            }
        }
    }

    private List<KafkaTopic> readTopics() throws Exception {
        if (ConfigReader.checkIfNodeExists(config, NODE_TOPICS)) {
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(NODE_TOPIC);
            List<KafkaTopic> topics = new ArrayList<>(nodes.size());
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                String name = node.getString(KafkaTopic.CONFIG_NAME);
                ConfigReader.checkStringValue(name, getClass(), KafkaTopic.CONFIG_NAME);
                KafkaTopic topic = new KafkaTopic();
                topic.name = name;
                if (node.containsKey(KafkaTopic.CONFIG_PARTITIONS)) {
                    String s = node.getString(KafkaTopic.CONFIG_PARTITIONS);
                    topic.partitions = Integer.parseInt(s);
                }
                if (node.containsKey(KafkaTopic.CONFIG_REPLICAS)) {
                    String s = node.getString(KafkaTopic.CONFIG_REPLICAS);
                    topic.replicas = Integer.parseInt(s);
                }
                if (node.containsKey(KafkaTopic.CONFIG_MIN_ISR)) {
                    String s = node.getString(KafkaTopic.CONFIG_MIN_ISR);
                    topic.minIsr = Integer.parseInt(s);
                }
                topics.add(topic);
            }
            if (!topics.isEmpty()) return topics;
        }
        return null;
    }

    public static void main(String[] args) {
        try {
            KafkaAdmin admin = new KafkaAdmin();
            JCommander.newBuilder().addObject(admin).build().parse(args);
            admin.run();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.LOGGER.error(t.getLocalizedMessage());
            t.printStackTrace();
        }
    }
}
