package ai.sapper.hcdc.core.messaging;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class MessageHandle<I, V> {
    private I id;
    private V data;
}
