package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ZookeeperSettings extends ConnectionSettings {
    @Setting(name = ZookeeperConnection.ZookeeperConfig.Constants.CONFIG_CONNECTION)
    private String connectionString;
    @Setting(name = ZookeeperConnection.ZookeeperConfig.Constants.CONFIG_AUTH_HANDLER, required = false)
    private String authenticationHandler;
    @Setting(name = ZookeeperConnection.ZookeeperConfig.Constants.CONFIG_NAMESPACE, required = false)
    private String namespace;
    @Setting(name = ZookeeperConnection.ZookeeperConfig.Constants.CONFIG_RETRY, required = false)
    private boolean retryEnabled = false;
    @Setting(name = ZookeeperConnection.ZookeeperConfig.Constants.CONFIG_RETRY_INTERVAL, required = false)
    private int retryInterval = 1000;
    @Setting(name = ZookeeperConnection.ZookeeperConfig.Constants.CONFIG_RETRY_TRIES, required = false)
    private int retryCount = 3;
    @Setting(name = ZookeeperConnection.ZookeeperConfig.Constants.CONFIG_CONN_TIMEOUT, required = false)
    private int connectionTimeout = -1;
    @Setting(name = ZookeeperConnection.ZookeeperConfig.Constants.CONFIG_SESSION_TIMEOUT, required = false)
    private int sessionTimeout = -1;

    public ZookeeperSettings() {
        setConnectionClass(ZookeeperConnection.class);
        setType(EConnectionType.zookeeper);
    }

    @Override
    public void validate() throws Exception {
        ConfigReader.checkStringValue(getName(), getClass(), ConnectionConfig.CONFIG_NAME);
        ConfigReader.checkStringValue(getConnectionString(), getClass(),
                ZookeeperConnection.ZookeeperConfig.Constants.CONFIG_CONNECTION);
    }
}
