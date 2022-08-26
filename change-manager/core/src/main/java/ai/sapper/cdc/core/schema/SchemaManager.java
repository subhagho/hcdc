package ai.sapper.cdc.core.schema;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.EntityDef;
import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.cdc.common.model.services.PathOrSchema;
import ai.sapper.cdc.common.model.services.PathWithSchema;
import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class SchemaManager {
    public static final String REGEX_PATH_VERSION = "(/.*)/(\\d+)/(\\d+)$";
    public static final String DEFAULT_DOMAIN = "default";
    private SchemaManagerConfig config;
    private ZookeeperConnection zkConnection;
    private DistributedLock lock;

    public SchemaManager() {
    }

    public SchemaManager(@NonNull SchemaManager schemaManager) {
        this.config = schemaManager.config;
        this.zkConnection = schemaManager.zkConnection;
        lock = new DistributedLock(SchemaManagerConfig.Constants.CONST_LOCK_NAMESPACE,
                SchemaManagerConfig.Constants.CONST_LOCK_NAME,
                config.basePath)
                .withConnection(zkConnection);
    }

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

    public EntityDef checkAndSave(@NonNull String schemaStr,
                                  @NonNull SchemaEntity schemaEntity) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaStr);
        return checkAndSave(schema, schemaEntity);
    }

    public EntityDef checkAndSave(@NonNull Schema schema,
                                  @NonNull SchemaEntity schemaEntity) throws Exception {
        SchemaVersion version = currentVersion(schemaEntity);
        EntityDef current = get(schemaEntity, version);
        if (current == null) {
            return save(schema, schemaEntity, version);
        } else {
            SchemaVersion next = nextVersion(schema, current.schema(), version);
            if (!next.equals(version)) {
                return save(schema, schemaEntity, next);
            }
        }
        return current;
    }

    public EntityDef save(@NonNull Schema schema,
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

            String p = getZkPath(schemaEntity);
            String json = JSONUtils.asString(version, SchemaVersion.class);
            client.setData().forPath(p, json.getBytes(StandardCharsets.UTF_8));

            return new EntityDef()
                    .schemaPath(p)
                    .schema(schema)
                    .version(version);
        } finally {
            lock.unlock();
        }
    }

    public EntityDef copySchema(@NonNull String source,
                                @NonNull SchemaEntity schemaEntity) throws Exception {
        EntityDef schema = get(source);
        if (schema != null) {
            return checkAndSave(schema.schema(), schemaEntity);
        }
        return null;
    }

    private SchemaVersion nextVersion(Schema schema,
                                      Schema current,
                                      SchemaVersion version) throws Exception {
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

    public boolean delete(@NonNull SchemaEntity schemaEntity,
                          @NonNull SchemaVersion version) throws Exception {
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

    public EntityDef get(@NonNull SchemaEntity schemaEntity) throws Exception {
        return get(schemaEntity, currentVersion(schemaEntity));
    }

    public EntityDef get(@NonNull SchemaEntity schemaEntity,
                         @NonNull SchemaVersion version) throws Exception {
        String path = getZkPath(schemaEntity, version);
        return get(path);
    }

    public String schemaPath(@NonNull SchemaEntity schemaEntity) throws Exception {
        return getZkPath(schemaEntity, currentVersion(schemaEntity));
    }

    public EntityDef get(@NonNull String path) throws Exception {
        CuratorFramework client = zkConnection().client();
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                SchemaVersion version = null;
                Pattern p = Pattern.compile(REGEX_PATH_VERSION);
                Matcher m = p.matcher(path);
                if (m.matches()) {
                    String mjv = m.group(2);
                    String mnv = m.group(3);
                    if (!Strings.isNullOrEmpty(mjv) && !Strings.isNullOrEmpty(mnv)) {
                        version = new SchemaVersion();
                        version.setMajorVersion(Integer.parseInt(mjv));
                        version.setMinorVersion(Integer.parseInt(mnv));
                    }
                }

                String schemaStr = new String(data, StandardCharsets.UTF_8);
                return new EntityDef().schemaPath(path)
                        .version(version)
                        .schema(new Schema.Parser().parse(schemaStr));
            }
        }
        return null;
    }

    private EntityDef get(@NonNull String path, SchemaVersion version) throws Exception {
        CuratorFramework client = zkConnection().client();
        path = PathUtils.formatZkPath(String.format("%s/%s", path, version.path()));
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                String schemaStr = new String(data, StandardCharsets.UTF_8);
                return new EntityDef()
                        .schema(new Schema.Parser().parse(schemaStr))
                        .version(version)
                        .schemaPath(path);
            }
        }
        return null;
    }

    private SchemaVersion currentVersion(SchemaEntity schemaEntity) throws Exception {
        CuratorFramework client = zkConnection().client();
        SchemaVersion version = new SchemaVersion();
        String path = getZkPath(schemaEntity);
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                version = JSONUtils.read(data, SchemaVersion.class);
            }
        }
        return version;
    }

    private SchemaVersion currentVersion(String path) throws Exception {
        CuratorFramework client = zkConnection().client();
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                return JSONUtils.read(data, SchemaVersion.class);
            }
        }
        return null;
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

    public List<PathOrSchema> domainNodes(String domain) throws Exception {
        if (Strings.isNullOrEmpty(domain)) {
            domain = DEFAULT_DOMAIN;
        }
        String path = String.format("%s/%s", config.basePath, domain);
        CuratorFramework client = zkConnection().client();
        if (client.checkExists().forPath(path) != null) {
            List<PathOrSchema> paths = new ArrayList<>();
            List<String> cPaths = client.getChildren().forPath(path);
            if (cPaths != null && !cPaths.isEmpty()) {
                for (String cp : cPaths) {
                    boolean added = false;
                    String zkPath = String.format("%s/%s", path, cp);
                    SchemaVersion version = currentVersion(zkPath);
                    if (version != null) {
                        EntityDef schema = get(zkPath, version);
                        if (schema != null) {
                            PathWithSchema ws = new PathWithSchema();
                            ws.setDomain(domain);
                            ws.setNode(cp);
                            ws.setZkPath(zkPath);
                            ws.setSchemaStr(schema.schema().toString(false));
                            ws.setVersion(JSONUtils.asString(version, SchemaVersion.class));
                            paths.add(ws);
                            added = true;
                        }
                    }
                    if (!added) {
                        PathOrSchema ps = new PathOrSchema();
                        ps.setDomain(domain);
                        ps.setNode(cp);
                        ps.setZkPath(zkPath);
                        paths.add(ps);
                    }
                }
            }
            if (!paths.isEmpty()) return paths;
        }
        return null;
    }

    public List<PathOrSchema> pathNodes(@NonNull String domain, @NonNull String path) throws Exception {
        CuratorFramework client = zkConnection().client();
        if (client.checkExists().forPath(path) != null) {
            List<PathOrSchema> paths = new ArrayList<>();
            List<String> cPaths = client.getChildren().forPath(path);
            if (cPaths != null && !cPaths.isEmpty()) {
                for (String cp : cPaths) {
                    boolean added = false;
                    String zkPath = String.format("%s/%s", path, cp);
                    SchemaVersion version = currentVersion(zkPath);
                    if (version != null) {
                        EntityDef schema = get(zkPath, version);
                        if (schema != null) {
                            PathWithSchema ws = new PathWithSchema();
                            ws.setDomain(domain);
                            ws.setNode(cp);
                            ws.setZkPath(zkPath);
                            ws.setSchemaStr(schema.schema().toString(false));
                            ws.setVersion(JSONUtils.asString(version, SchemaVersion.class));
                            paths.add(ws);
                            added = true;
                        }
                    }
                    if (!added) {
                        PathOrSchema ps = new PathOrSchema();
                        ps.setDomain(domain);
                        ps.setNode(cp);
                        ps.setZkPath(zkPath);
                        paths.add(ps);
                    }
                }
            }
            if (!paths.isEmpty()) return paths;
        }
        return null;
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
