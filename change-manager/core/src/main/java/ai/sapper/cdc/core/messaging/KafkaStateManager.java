package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.curator.framework.CuratorFramework;

@Getter
@Accessors(fluent = true)
public class KafkaStateManager {
    private final String name;
    private final String topic;
    private final ZookeeperConnection zkConnection;
    private final String zkStatePath;

    public KafkaStateManager(@NonNull String name,
                             @NonNull String topic,
                             @NonNull ZookeeperConnection zkConnection,
                             @NonNull String zkStatePath) {
        this.name = name;
        this.topic = topic;
        this.zkConnection = zkConnection;
        this.zkStatePath = zkStatePath;
    }


    public KafkaMessageState getState(long partition) throws Exception {
        String path = getZkPath(partition);
        CuratorFramework client = zkConnection().client();

        KafkaMessageState state = null;
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(path);

            state = new KafkaMessageState();
            state.setName(name);
            state.setPath(path);
            state.setTopic(topic);
            state.setPartition(partition);
            state.setUpdateTimestamp(System.currentTimeMillis());

            byte[] data = JSONUtils.asBytes(state, KafkaMessageState.class);
            client.setData().forPath(path, data);
        } else {
            byte[] data = client.getData().forPath(path);
            if (data == null || data.length == 0) {
                throw new MessagingError(String.format("Invalid Kafka Message state. [path=%s]", path));
            }
            state = JSONUtils.read(data, KafkaMessageState.class);
        }

        return state;
    }

    public KafkaMessageState updateState(long partition, long offset) throws Exception {
        KafkaMessageState state = getState(partition);
        state.setOffset(offset);
        state.setUpdateTimestamp(System.currentTimeMillis());

        CuratorFramework client = zkConnection().client();
        String path = getZkPath(partition);
        byte[] data = JSONUtils.asBytes(state, KafkaMessageState.class);
        client.setData().forPath(path, data);

        return state;
    }

    private String getZkPath(long partition) {
        return new PathUtils.ZkPathBuilder(zkStatePath)
                .withPath(name)
                .withPath(topic)
                .withPath(String.valueOf(partition))
                .build();
    }
}
