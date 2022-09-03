package ai.sapper.cdc.core.connections.settngs;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public abstract class ConnectionSettings {
    private String name;
    private Map<String, String> parameters;
    private Class<?> connectionType;
}
