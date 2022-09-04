package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.core.connections.Connection;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public abstract class ConnectionSettings {
    private EConnectionType type;
    private ESettingsSource source;
    private String name;
    private Map<String, String> parameters;
    private Class<? extends Connection> connectionClass;

    public ConnectionSettings() {
        source = ESettingsSource.File;
    }
}
