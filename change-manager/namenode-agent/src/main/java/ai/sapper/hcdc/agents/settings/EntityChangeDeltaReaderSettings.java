package ai.sapper.hcdc.agents.settings;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.EncryptionHandler;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.nio.ByteBuffer;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class EntityChangeDeltaReaderSettings extends ChangeDeltaProcessorSettings {
    public static class Constants {
        public static final String CONFIG_HDFS_CONN = "hdfs";
        public static final String CONFIG_WS_PATH = "snapshot";
        public static final String CONFIG_ARCHIVER_CLASS = "archiver.class";
        public static final String CONFIG_ENCRYPTOR_CLASS = "encryption.class";
    }

    @Config(name = Constants.CONFIG_HDFS_CONN)
    private String hdfsConnection;
    @Config(name = Constants.CONFIG_ARCHIVER_CLASS, required = false, type = Class.class)
    private Class<? extends Archiver> archiverClass;
    private HierarchicalConfiguration<ImmutableNode> fsConfig;
    @Config(name = Constants.CONFIG_ENCRYPTOR_CLASS, required = false, type = Class.class)
    private Class<? extends EncryptionHandler<ByteBuffer, ByteBuffer>> encryptorClass;
}
