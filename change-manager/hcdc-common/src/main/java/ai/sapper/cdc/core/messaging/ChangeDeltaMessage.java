package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.messaging.IMessage;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class ChangeDeltaMessage implements IMessage<String, DFSChangeDelta> {
    private String key;
    private DFSChangeDelta data;

    public void setKey(@NonNull String key) {
        this.key = key;
    }

    public void setData(@NonNull DFSChangeDelta data) {
        this.data = data;
    }

    /**
     * @return
     */
    @Override
    public String key() {
        return key;
    }

    /**
     * @return
     */
    @Override
    public DFSChangeDelta value() {
        return data;
    }
}
