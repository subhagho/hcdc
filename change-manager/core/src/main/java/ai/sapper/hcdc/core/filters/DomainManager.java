package ai.sapper.hcdc.core.filters;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.SchemaEntity;
import ai.sapper.hcdc.common.model.filters.DomainFilter;
import ai.sapper.hcdc.common.model.filters.DomainFilterMatcher;
import ai.sapper.hcdc.common.model.filters.DomainFilters;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.common.utils.JSONUtils;
import ai.sapper.hcdc.common.utils.PathUtils;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class DomainManager {
    private static final String CONFIG_PATH = "domain";

    private ZookeeperConnection zkConnection;
    private HdfsConnection hdfsConnection;

    private DomainManagerConfig config;
    private Map<String, DomainFilterMatcher> matchers = new HashMap<>();
    private final List<FilterAddCallback> callbacks = new ArrayList<>();

    public DomainManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            config = new DomainManagerConfig(xmlConfig);
            config.read();

            zkConnection = manger.getConnection(config.zkConnection, ZookeeperConnection.class);
            if (zkConnection == null) {
                throw new ConfigurationException(
                        String.format("ZooKeeper connection not found. [name=%s]", config.zkConnection));
            }
            if (!zkConnection.isConnected()) zkConnection.connect();

            if (!Strings.isNullOrEmpty(config.hdfsConnection)) {
                hdfsConnection = manger.getConnection(config.hdfsConnection, HdfsConnection.class);
                if (hdfsConnection == null) {
                    throw new ConfigurationException(
                            String.format("HDFS Connection not found. [name=%s]", config.hdfsConnection));
                }
                if (!hdfsConnection.isConnected()) {
                    hdfsConnection.connect();
                }
            }
            String path = getZkPath();
            CuratorFramework client = zkConnection.client();
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentContainersIfNeeded().forPath(path);
            }
            readFilters();

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public DomainManager withFilterAddCallback(@NonNull FilterAddCallback callback) {
        callbacks.add(callback);
        return this;
    }

    private void readFilters() throws Exception {
        String path = getZkPath();
        CuratorFramework client = zkConnection.client();

        if (client.checkExists().forPath(path) != null) {
            List<String> paths = client.getChildren().forPath(path);
            if (paths != null && !paths.isEmpty()) {
                for (String p : paths) {
                    String dp = getZkPath(p);
                    byte[] data = client.getData().forPath(dp);
                    if (data != null && data.length > 0) {
                        String json = new String(data, StandardCharsets.UTF_8);
                        DomainFilters df = JSONUtils.read(json, DomainFilters.class);
                        DomainFilterMatcher m = new DomainFilterMatcher(df.getDomain(), df);
                        matchers.put(df.getDomain(), m);
                        if (!callbacks.isEmpty()) {
                            for (FilterAddCallback callback : callbacks) {
                                callback.onStart(m);
                            }
                        }
                    }
                }
                if (!matchers.isEmpty()) {
                    this.matchers = matchers;
                }
            }
        }
    }

    private String getZkPath() {
        return PathUtils.formatZkPath(String.format("%s/%s", config.basePath, CONFIG_PATH));
    }

    private String getZkPath(String domain) {
        return PathUtils.formatZkPath(String.format("%s/%s", getZkPath(), domain));
    }

    public SchemaEntity matches(@NonNull String path) {
        Preconditions.checkNotNull(zkConnection);
        Preconditions.checkState(zkConnection.isConnected());

        if (matchers != null && !matchers.isEmpty()) {
            Map<String, DomainFilterMatcher> ms = matchers;
            for (String d : matchers.keySet()) {
                DomainFilterMatcher m = ms.get(d);
                DomainFilterMatcher.PathFilter pf = m.matches(path);
                if (pf != null) {
                    SchemaEntity dd = new SchemaEntity();
                    dd.setDomain(m.filters().getDomain());
                    dd.setEntity(pf.filter().getEntity());
                    return dd;
                }
            }
        }
        return null;
    }

    public DomainFilters add(@NonNull String domain, @NonNull String entity, @NonNull String path, @NonNull String regex) throws Exception {
        Preconditions.checkNotNull(zkConnection);
        Preconditions.checkState(zkConnection.isConnected());

        DomainFilterMatcher matcher = null;
        DomainFilterMatcher.PathFilter filter = null;
        if (!matchers.containsKey(domain)) {
            DomainFilters df = new DomainFilters();
            df.setDomain(domain);
            DomainFilter d = df.add(entity, path, regex);

            matcher = new DomainFilterMatcher(domain, df);
            matchers.put(domain, matcher);
            filter = matcher.find(d);
        } else {
            matcher = matchers.get(domain);
            filter = matcher.add(entity, path, regex);
        }

        CuratorFramework client = zkConnection.client();
        String json = JSONUtils.asString(matcher.filters(), DomainFilters.class);
        String zp = getZkPath(domain);
        if (client.checkExists().forPath(zp) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zp);
        }
        Stat stat = client.setData().forPath(zp, json.getBytes(StandardCharsets.UTF_8));
        DefaultLogger.LOG.debug(String.format("Added Domain Filter: [path=%s][filter=%s]", zp, json));
        if (!callbacks.isEmpty()) {
            for (FilterAddCallback callback : callbacks) {
                callback.process(matcher, filter, path);
            }
        }
        return matcher.filters();
    }

    @Getter
    @Accessors(fluent = true)
    public static class DomainManagerConfig extends ConfigReader {
        public static final class Constants {
            public static final String CONFIG_ZK_BASE = "basePath";
            public static final String CONFIG_ZK_CONNECTION = "connection";
            public static final String CONFIG_HDFS_CONNECTION = "hdfs";
        }

        private static final String __CONFIG_PATH = "domain.manager";

        private String basePath;
        private String zkConnection;
        private String hdfsConnection;

        public DomainManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public DomainManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String configPath) {
            super(config, configPath);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not drt or is NULL");
            }
            try {
                basePath = get().getString(Constants.CONFIG_ZK_BASE);
                if (Strings.isNullOrEmpty(basePath)) {
                    throw new ConfigurationException(String.format("State Manager Configuration Error: missing [%s]", Constants.CONFIG_ZK_BASE));
                }
                basePath = basePath.trim();
                if (basePath.endsWith("/")) {
                    basePath = basePath.substring(0, basePath.length() - 2);
                }
                zkConnection = get().getString(Constants.CONFIG_ZK_CONNECTION);
                if (Strings.isNullOrEmpty(zkConnection)) {
                    throw new ConfigurationException(String.format("State Manager Configuration Error: missing [%s]", Constants.CONFIG_ZK_CONNECTION));
                }
                if (get().containsKey(Constants.CONFIG_HDFS_CONNECTION)) {
                    hdfsConnection = get().getString(Constants.CONFIG_HDFS_CONNECTION);
                }
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing State Manager configuration.", t);
            }
        }
    }
}
