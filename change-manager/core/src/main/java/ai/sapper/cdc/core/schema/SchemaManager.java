package ai.sapper.cdc.core.schema;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.cache.LRUCache;
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
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Level;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class SchemaManager {
    @Getter
    @Setter
    @Accessors(fluent = true)
    private static class SchemaElement {
        private EntityDef entityDef;
        private long readtime;
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    private static class CacheEntity {
        private Map<SchemaVersion, SchemaElement> versions = new HashMap<>();

        public EntityDef get(SchemaVersion version, long timeout) {
            if (versions.containsKey(version)) {
                SchemaElement se = versions.get(version);
                long t = System.currentTimeMillis() - se.readtime;
                if (t < timeout) return se.entityDef;
                else {
                    versions.remove(version);
                }
            }
            return null;
        }

        public void put(SchemaVersion version, EntityDef entityDef) {
            SchemaElement se = versions.get(version);
            if (se == null) {
                se = new SchemaElement();
                versions.put(version, se);
            }
            se.entityDef(entityDef);
            se.readtime(System.currentTimeMillis());
        }
    }

    public static final String REGEX_PATH_VERSION = "(/.*)/(\\d+)/(\\d+)$";
    public static final String DEFAULT_DOMAIN = "default";
    public static final String SCHEMA_PATH = "schemas";

    private SchemaManagerConfig config;
    private ZookeeperConnection zkConnection;
    private DistributedLock writeLock;
    private String environment;
    private String source;
    private String zkPath;
    private String lockPath;
    private LRUCache<SchemaEntity, CacheEntity> cache = null;

    public SchemaManager() {
    }

    public SchemaManager(@NonNull SchemaManager schemaManager) {
        this.config = schemaManager.config;
        this.zkConnection = schemaManager.zkConnection;
        this.zkPath = schemaManager.zkPath;
        this.environment = schemaManager.environment;
        this.source = schemaManager.source;
        this.lockPath = schemaManager.lockPath;

        createWriteLock();
    }

    public SchemaManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull ConnectionManager manger,
                              @NonNull String environment,
                              @NonNull String source,
                              @NonNull String lockPath) throws ConfigurationException {
        try {
            config = new SchemaManagerConfig(xmlConfig);
            config.read();

            this.environment = environment;
            this.source = source;
            this.lockPath = lockPath;

            zkConnection = manger.getConnection(config.connection(), ZookeeperConnection.class);
            if (zkConnection == null) {
                throw new ConfigurationException(
                        String.format("Zookeeper Connection not found. [name=%s]", config.connection));
            }
            if (!zkConnection().isConnected()) zkConnection.connect();

            zkPath = new PathUtils.ZkPathBuilder(config.basePath)
                    .withPath(environment)
                    .withPath(SCHEMA_PATH)
                    .withPath(source)
                    .build();
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(zkPath) == null) {
                client.create().creatingParentsIfNeeded().forPath(zkPath);
            }
            createWriteLock();
            if (config.cached) {
                cache = new LRUCache<>(config.cacheSize);
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void createWriteLock() {
        writeLock = new DistributedLock(SchemaManagerConfig.Constants.CONST_LOCK_NAMESPACE,
                SchemaManagerConfig.Constants.CONST_LOCK_NAME,
                lockPath)
                .withConnection(zkConnection);
    }

    private EntityDef checkCache(SchemaEntity schemaEntity, SchemaVersion version) {
        if (cache != null) {
            Optional<CacheEntity> ce = cache.get(schemaEntity);
            if (ce.isPresent()) {
                return ce.get().get(version, config.cacheTimeout);
            }
        }
        return null;
    }

    private void putInCache(SchemaEntity schemaEntity, SchemaVersion version, EntityDef entityDef) {
        if (cache != null) {
            CacheEntity c = null;
            Optional<CacheEntity> ce = cache.get(schemaEntity);
            if (ce.isPresent()) {
                c = ce.get();
            } else {
                c = new CacheEntity();
                cache.put(schemaEntity, c);
            }
            c.put(version, entityDef);
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
        writeLock.lock();
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

            EntityDef en = new EntityDef()
                    .schemaPath(p)
                    .schema(schema)
                    .version(version);
            putInCache(schemaEntity, version, en);
            return en;
        } finally {
            writeLock.unlock();
        }
    }

    public EntityDef copySchema(@NonNull String source,
                                @NonNull SchemaEntity schemaEntity) throws Exception {
        EntityDef schema = get(schemaEntity, source);
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
        writeLock.lock();
        try {
            String path = getZkPath(schemaEntity, version);
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean delete(@NonNull SchemaEntity schemaEntity) throws Exception {
        writeLock.lock();
        try {
            String path = getZkPath(schemaEntity);
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    public EntityDef get(@NonNull SchemaEntity schemaEntity) throws Exception {
        return get(schemaEntity, currentVersion(schemaEntity));
    }

    public EntityDef get(@NonNull SchemaEntity schemaEntity,
                         @NonNull SchemaVersion version) throws Exception {
        EntityDef en = checkCache(schemaEntity, version);
        if (en != null) return en;

        String path = getZkPath(schemaEntity, version);
        return get(schemaEntity, path);
    }

    public String schemaPath(@NonNull SchemaEntity schemaEntity) throws Exception {
        return getZkPath(schemaEntity, currentVersion(schemaEntity));
    }

    public EntityDef get(@NonNull SchemaEntity schemaEntity,
                         @NonNull String path) throws Exception {
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
                EntityDef en = new EntityDef().schemaPath(path)
                        .version(version)
                        .schema(new Schema.Parser().parse(schemaStr));
                putInCache(schemaEntity, version, en);
                return en;
            }
        }
        return null;
    }

    private EntityDef get(@NonNull String path, SchemaVersion version) throws Exception {
        CuratorFramework client = zkConnection().client();
        path = new PathUtils.ZkPathBuilder(path)
                .withPath(version.path())
                .build();
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

    private String getZkPath() {
        return new PathUtils.ZkPathBuilder(zkPath)
                .withPath(config().schema)
                .build();
    }

    private String getZkDomainPath(String domain) {
        return new PathUtils.ZkPathBuilder(getZkPath())
                .withPath(domain)
                .build();
    }

    private String getZkPath(SchemaEntity schema) {
        return new PathUtils.ZkPathBuilder(getZkPath())
                .withPath(schema.getDomain())
                .withPath(schema.getEntity())
                .build();
    }

    private String getZkPath(SchemaEntity schema, SchemaVersion version) {
        return new PathUtils.ZkPathBuilder(getZkPath(schema))
                .withPath(version.path())
                .build();
    }

    public List<PathOrSchema> domainNodes(String domain) throws Exception {
        if (Strings.isNullOrEmpty(domain)) {
            domain = DEFAULT_DOMAIN;
        }
        String path = getZkDomainPath(domain);
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
            public static final String CONFIG_CACHE = "cache";
            public static final String CONFIG_SCHEMA = "schema";
            public static final String CONFIG_CACHED = String.format("%s.enable", CONFIG_CACHE);
            public static final String CONFIG_CACHE_EXPIRY = String.format("%s.expire", CONFIG_CACHE);
            public static final String CONFIG_CACHE_SIZE = String.format("%s.size", CONFIG_CACHE);
        }

        private String connection;
        private String basePath;
        private String schema;

        private boolean cached = true;
        private long cacheTimeout = 1000 * 60 * 30; // 30 mins
        private int cacheSize = 1024;

        public SchemaManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, Constants.__CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Schema Manager Configuration not set or is NULL");
            }
            connection = get().getString(Constants.CONFIG_CONNECTION);
            if (Strings.isNullOrEmpty(connection)) {
                throw new ConfigurationException(
                        String.format("Schema Manager Configuration Error: missing [%s]", Constants.CONFIG_CONNECTION));
            }
            basePath = get().getString(Constants.CONFIG_BASE_PATH);
            if (Strings.isNullOrEmpty(basePath)) {
                throw new ConfigurationException(
                        String.format("Schema Manager Configuration Error: missing [%s]", Constants.CONFIG_BASE_PATH));
            }
            schema = get().getString(Constants.CONFIG_SCHEMA);
            if (Strings.isNullOrEmpty(schema)) {
                throw new ConfigurationException(
                        String.format("Schema Manager Configuration Error: missing [%s]", Constants.CONFIG_SCHEMA));
            }
            if (ConfigReader.checkIfNodeExists(get(), Constants.CONFIG_CACHE)) {
                String s = get().getString(Constants.CONFIG_CACHED);
                if (!Strings.isNullOrEmpty(s)) {
                    cached = Boolean.parseBoolean(s);
                }
                s = get().getString(Constants.CONFIG_CACHE_EXPIRY);
                if (!Strings.isNullOrEmpty(s)) {
                    cacheTimeout = Long.parseLong(s);
                }
                s = get().getString(Constants.CONFIG_CACHE_SIZE);
                if (!Strings.isNullOrEmpty(s)) {
                    cacheSize = Integer.parseInt(s);
                }
            }
        }
    }
}
