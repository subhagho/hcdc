package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.kafka.KafkaConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;

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
    public static final String PROP_CLIENT_ID = "client.id";

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

    public KafkaSettings(@NonNull KafkaSettings settings) {
        super(settings);
        setType(EConnectionType.kafka);
        configPath = settings.configPath;
        if (settings.properties != null) {
            properties = new Properties(settings.properties.size());
            properties.putAll(settings.properties);
        }
        mode = settings.mode;
        topic = settings.topic;
        if (settings.partitions != null) {
            partitions = new ArrayList<>(settings.partitions);
        }
    }

    public KafkaSettings clientId(@NonNull String clientId) {
        properties.put(PROP_CLIENT_ID, clientId);
        return this;
    }

    public String clientId() {
        if (properties != null) {
            return properties.getProperty(PROP_CLIENT_ID);
        }
        return null;
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
