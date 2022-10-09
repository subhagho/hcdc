package ai.sapper.cdc.core.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Heartbeat {
    private String name;
    private ModuleInstance module;
    private String type;
    private String state;
    private Throwable error;
    private long timestamp;
}
