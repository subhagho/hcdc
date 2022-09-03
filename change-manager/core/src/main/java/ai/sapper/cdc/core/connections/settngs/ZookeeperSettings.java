package ai.sapper.cdc.core.connections.settngs;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ZookeeperSettings {
    private String name;
    private String connectionString;
    private String authenticationHandler;
    private String namespace;
    private boolean retryEnabled = false;
    private int retryInterval = 1000;
    private int retryCount = 3;
    private int connectionTimeout = -1;
    private int sessionTimeout = -1;
}
