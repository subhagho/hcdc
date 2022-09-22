package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.kafka.KafkaConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KafkaSettings extends ConnectionSettings {
    @Setting(name = "configPath")
    private String configPath;
    private Properties properties;
    @Setting(name = KafkaConnection.KafkaConfig.Constants.CONFIG_MODE, required = false)
    private KafkaConnection.EKafkaClientMode mode = KafkaConnection.EKafkaClientMode.Producer;
    @Setting(name = KafkaConnection.KafkaConfig.Constants.CONFIG_TOPIC, required = false)
    private String topic;
    @Setting(name = KafkaConnection.KafkaConfig.Constants.CONFIG_PARTITIONS, required = false, parser = KafkaPartitionsParser.class)
    private List<Integer> partitions;

    public KafkaSettings() {
        setType(EConnectionType.kafka);
    }

    @Override
    public void validate() throws Exception {
        ConfigReader.checkStringValue(getName(), getClass(), ConnectionConfig.CONFIG_NAME);
        if (getProperties() != null) {
            File configFile = ConfigReader.readFileNode(configPath);
            if (configFile == null || !configFile.exists()) {
                throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]",
                        KafkaConnection.KafkaConfig.Constants.CONFIG_FILE_CONFIG));
            }
            setProperties(new Properties());
            getProperties().load(new FileInputStream(configFile));
        }
    }
}
