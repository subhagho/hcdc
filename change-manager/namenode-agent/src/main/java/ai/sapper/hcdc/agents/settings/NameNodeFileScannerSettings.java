package ai.sapper.hcdc.agents.settings;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.io.EncryptionHandler;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.nio.ByteBuffer;

@Getter
@Setter
public class NameNodeFileScannerSettings extends Settings {
    public static class Constants {
        public static final String CONFIG_HDFS_CONN = "hdfs";
        public static final String CONFIG_SHARD_PATH = "shards";
        public static final String CONFIG_SHARD_COUNT = "shards.count";
        public static final String CONFIG_SHARD_ID = "shards.id";
        public static final String CONFIG_THREAD_COUNT = "threads";
        public static final String CONFIG_ENCRYPTOR_CLASS = "encryption.class";
    }

    @Config(name = Constants.CONFIG_HDFS_CONN)
    private String hdfsConnection;
    private HierarchicalConfiguration<ImmutableNode> fsConfig;
    @Config(name = Constants.CONFIG_SHARD_COUNT, required = false, type = Integer.class)
    private int shardCount = 1;
    @Config(name = Constants.CONFIG_SHARD_ID, required = false, type = Integer.class)
    private int shardId = 0;
    @Config(name = Constants.CONFIG_THREAD_COUNT, required = false, type = Integer.class)
    private int threads = 1;
    @Config(name = Constants.CONFIG_ENCRYPTOR_CLASS, required = false, type = Class.class)
    private Class<? extends EncryptionHandler<ByteBuffer, ByteBuffer>> encryptorClass;
}
