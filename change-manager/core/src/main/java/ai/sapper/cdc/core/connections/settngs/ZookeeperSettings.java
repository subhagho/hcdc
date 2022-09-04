package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.core.connections.ZookeeperConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ZookeeperSettings extends ConnectionSettings {
    private String connectionString;
    private String authenticationHandler;
    private String namespace;
    private boolean retryEnabled = false;
    private int retryInterval = 1000;
    private int retryCount = 3;
    private int connectionTimeout = -1;
    private int sessionTimeout = -1;

    public ZookeeperSettings() {
        setConnectionClass(ZookeeperConnection.class);
        setType(EConnectionType.zookeeper);
    }
}
