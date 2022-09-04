package ai.sapper.cdc.core.connections;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConnectionOpRequest {
    private String name;
    private Class<? extends Connection> connectionClass;
}
