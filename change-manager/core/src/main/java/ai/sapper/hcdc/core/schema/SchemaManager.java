package ai.sapper.hcdc.core.schema;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.SchemaEntity;
import ai.sapper.hcdc.common.schema.SchemaVersion;
import ai.sapper.hcdc.common.utils.PathUtils;
import ai.sapper.hcdc.core.DistributedLock;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Level;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class SchemaManager {
    private SchemaManagerConfig config;
    private ZookeeperConnection zkConnection;
    private DistributedLock lock;

    public SchemaManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            config = new SchemaManagerConfig(xmlConfig);
            config.read();

            zkConnection = manger.getConnection(config.connection(), ZookeeperConnection.class);
            if (zkConnection == null) {
                throw new ConfigurationException(
                        String.format("Zookeeper Connection not found. [name=%s]", config.connection));
            }
            if (!zkConnection().isConnected()) zkConnection.connect();

            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(config().basePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(config.basePath);
            }
            lock = new DistributedLock(SchemaManagerConfig.Constants.CONST_LOCK_NAMESPACE,
                    SchemaManagerConfig.Constants.CONST_LOCK_NAME,
                    config.basePath)
                    .withConnection(zkConnection);
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public String checkAndSave(@NonNull String schemaStr, @NonNull SchemaEntity schemaEntity) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaStr);
        return checkAndSave(schema, schemaEntity);
    }

    public String checkAndSave(@NonNull Schema schema, @NonNull SchemaEntity schemaEntity) throws Exception {
        SchemaVersion version = currentVersion(schemaEntity);
        Schema current = get(schemaEntity, version);
        if (current == null) {
            return save(schema, schemaEntity, version);
        } else {
            SchemaVersion next = nextVersion(schema, current, version);
            if (!next.equals(version)) {
                return save(schema, schemaEntity, next);
            }
        }
        return getZkPath(schemaEntity, version);
    }

    public String save(@NonNull Schema schema,
                       @NonNull SchemaEntity schemaEntity,
                       @NonNull SchemaVersion version) throws Exception {
        lock.lock();
        try {
            String path = getZkPath(schemaEntity, version);
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
            String schemaStr = schema.toString(false);
            client.setData().forPath(path, schemaStr.getBytes(StandardCharsets.UTF_8));
            return path;
        } finally {
            lock.unlock();
        }
    }

    private SchemaVersion nextVersion(Schema schema, Schema current, SchemaVersion version) throws Exception {
        SchemaVersion next = new SchemaVersion(version);
        if (current != null) {
            if (schema.equals(current)) return next;

            List<SchemaEvolutionValidator.Message> messages
                    = SchemaEvolutionValidator.checkBackwardCompatibility(current, schema, schema.getName());
            Level maxLevel = Level.ALL;
            for (SchemaEvolutionValidator.Message message : messages) {
                if (message.getLevel().isGreaterOrEqual(maxLevel)) {
                    maxLevel = message.getLevel();
                }
            }

            if (maxLevel.isGreaterOrEqual(Level.ERROR)) {
                next.setMajorVersion(version.getMajorVersion() + 1);
                next.setMinorVersion(0);
            } else {
                next.setMinorVersion(version.getMinorVersion() + 1);
            }
        }
        return next;
    }

    public boolean delete(@NonNull SchemaEntity schemaEntity, @NonNull SchemaVersion version) throws Exception {
        lock.lock();
        try {
            String path = getZkPath(schemaEntity, version);
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public boolean delete(@NonNull SchemaEntity schemaEntity) throws Exception {
        lock.lock();
        try {
            String path = getZkPath(schemaEntity);
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public Schema get(@NonNull SchemaEntity schemaEntity) throws Exception {
        return get(schemaEntity, currentVersion(schemaEntity));
    }

    public Schema get(@NonNull SchemaEntity schemaEntity, @NonNull SchemaVersion version) throws Exception {
        String path = getZkPath(schemaEntity, version);
        CuratorFramework client = zkConnection().client();
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                String schemaStr = new String(data, StandardCharsets.UTF_8);
                return new Schema.Parser().parse(schemaStr);
            }
        }
        return null;
    }

    private SchemaVersion currentVersion(SchemaEntity schemaEntity) throws Exception {
        CuratorFramework client = zkConnection().client();
        SchemaVersion version = new SchemaVersion();
        String path = getZkPath(schemaEntity);
        if (client.checkExists().forPath(path) != null) {
            int majV = 0;
            int minV = 1;
            List<String> mjC = client.getChildren().forPath(path);
            if (mjC != null && !mjC.isEmpty()) {
                for (String v : mjC) {
                    int ii = Integer.parseInt(v);
                    if (ii > majV) {
                        majV = ii;
                    }
                }
                path = String.format("%s/%d", path, majV);
                if (client.checkExists().forPath(path) != null) {
                    List<String> mnC = client.getChildren().forPath(path);
                    if (mnC != null && !mnC.isEmpty()) {
                        for (String v : mnC) {
                            int ii = Integer.parseInt(v);
                            if (ii > minV) {
                                minV = ii;
                            }
                        }
                    }
                }
            }
            version.setMajorVersion(majV);
            version.setMinorVersion(minV);
        }
        return version;
    }

    private String getZkPath(SchemaEntity schema) {
        String path = String.format("%s/%s/%s", config.basePath, schema.getDomain(), schema.getEntity());
        return PathUtils.formatZkPath(path);
    }

    private String getZkPath(SchemaEntity schema, SchemaVersion version) {
        String path = String.format("%s/%s/%s/%s",
                config.basePath, schema.getDomain(), schema.getEntity(), version.path());
        return PathUtils.formatZkPath(path);
    }

    @Getter
    @Accessors(fluent = true)
    public static class SchemaManagerConfig extends ConfigReader {
        public static class Constants {
            public static final String CONST_LOCK_NAMESPACE = SchemaManager.class.getSimpleName();
            public static final String CONST_LOCK_NAME = "__lock";

            public static final String __CONFIG_PATH = "managers.schema";
            public static final String CONFIG_CONNECTION = "connection";
            public static final String CONFIG_BASE_PATH = "basePath";
        }

        private String connection;
        private String basePath;

        public SchemaManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, Constants.__CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Domain Manager Configuration not set or is NULL");
            }
            connection = get().getString(Constants.CONFIG_CONNECTION);
            if (Strings.isNullOrEmpty(connection)) {
                throw new ConfigurationException(
                        String.format("Domain Manager Configuration Error: missing [%s]", Constants.CONFIG_CONNECTION));
            }
            basePath = get().getString(Constants.CONFIG_BASE_PATH);
            if (Strings.isNullOrEmpty(basePath)) {
                throw new ConfigurationException(
                        String.format("Domain Manager Configuration Error: missing [%s]", Constants.CONFIG_BASE_PATH));
            }
        }
    }
}
