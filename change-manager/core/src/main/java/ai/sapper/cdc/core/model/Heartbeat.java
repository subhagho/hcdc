package ai.sapper.cdc.core.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Heartbeat {
    private String name;
    private ModuleInstance module;
    private String type;
    private String state;
    private Throwable error;
    private long timestamp;
}
