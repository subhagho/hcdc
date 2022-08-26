package ai.sapper.cdc.common.audit;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AuditRecord<T> {
    private String type;
    private String caller;
    private long timestamp;
    private T data;
}
