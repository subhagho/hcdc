package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.core.connections.kafka.KafkaConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Properties;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KafkaSettings extends ConnectionSettings {
    private String configPath;
    private Properties properties;
    private KafkaConnection.EKafkaClientMode mode = KafkaConnection.EKafkaClientMode.Producer;
    private String topic;

    private List<Integer> partitions;
}
