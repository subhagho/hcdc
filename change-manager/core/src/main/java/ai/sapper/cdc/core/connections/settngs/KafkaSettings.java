package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.core.connections.KafkaConnection;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.Properties;

@Getter
@Setter
public class KafkaSettings {
    private String name;
    private String configPath;
    private Properties properties;
    private KafkaConnection.EKafkaClientMode mode = KafkaConnection.EKafkaClientMode.Producer;
    private String topic;

    private List<Integer> partitions;
    private Map<String, String> parameters;
}
