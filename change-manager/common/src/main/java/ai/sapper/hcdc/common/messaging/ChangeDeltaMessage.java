package ai.sapper.hcdc.common.messaging;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
public class ChangeDeltaMessage implements IMessage<String, ChangeDeltaData> {
    private String key;
    private ChangeDeltaData data;

    public void setKey(@NonNull String key) {
        this.key = key;
    }

    public void setData(@NonNull ChangeDeltaData data) {
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
    public ChangeDeltaData value() {
        return data;
    }
}
