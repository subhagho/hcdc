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
    @Parameter(names = {"--match", "-m"}, description = "Delete topics matching specified RegEx.")
    private String regex;
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

        List<KafkaAdminHelper.KafkaTopic> topics = readTopics();

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

    private void runCreate(List<KafkaAdminHelper.KafkaTopic> topics) throws Exception {
        if (topics != null && !topics.isEmpty()) {
            for (KafkaAdminHelper.KafkaTopic topic : topics) {
                if (helper.exists(topic.name())) {
                    throw new Exception(String.format("Kafka Topic already exists. [name=%s]", topic.name()));
                }
                helper.createTopic(topic);
            }
        }
    }

    private void searchDelete() throws Exception {
        List<KafkaAdminHelper.KafkaTopic> topics = helper.search(regex);
        if (topics != null && !topics.isEmpty()) {
            delete(topics);
        }
    }

    private void runDelete(List<KafkaAdminHelper.KafkaTopic> topics) throws Exception {
        if (!Strings.isNullOrEmpty(regex)) {
            searchDelete();
        } else {
            delete(topics);
        }
    }

    private void delete(List<KafkaAdminHelper.KafkaTopic> topics) throws Exception {
        if (topics != null && !topics.isEmpty()) {
            for (KafkaAdminHelper.KafkaTopic topic : topics) {
                if (helper.exists(topic.name())) {
                    helper.deleteTopic(topic.name());
                }
            }
        }
    }

    private List<KafkaAdminHelper.KafkaTopic> readTopics() throws Exception {
        if (ConfigReader.checkIfNodeExists(config, NODE_TOPICS)) {
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(NODE_TOPIC);
            List<KafkaAdminHelper.KafkaTopic> topics = new ArrayList<>(nodes.size());
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                String name = node.getString(KafkaAdminHelper.KafkaTopic.CONFIG_NAME);
                ConfigReader.checkStringValue(name, getClass(), KafkaAdminHelper.KafkaTopic.CONFIG_NAME);
                KafkaAdminHelper.KafkaTopic topic = new KafkaAdminHelper.KafkaTopic();
                topic.name(name);
                if (node.containsKey(KafkaAdminHelper.KafkaTopic.CONFIG_PARTITIONS)) {
                    String s = node.getString(KafkaAdminHelper.KafkaTopic.CONFIG_PARTITIONS);
                    topic.partitions(Integer.parseInt(s));
                }
                if (node.containsKey(KafkaAdminHelper.KafkaTopic.CONFIG_REPLICAS)) {
                    String s = node.getString(KafkaAdminHelper.KafkaTopic.CONFIG_REPLICAS);
                    topic.replicas(Short.parseShort(s));
                }
                if (node.containsKey(KafkaAdminHelper.KafkaTopic.CONFIG_MIN_ISR)) {
                    String s = node.getString(KafkaAdminHelper.KafkaTopic.CONFIG_MIN_ISR);
                    topic.minIsr(Short.parseShort(s));
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
