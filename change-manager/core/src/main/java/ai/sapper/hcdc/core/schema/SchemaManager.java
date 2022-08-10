package ai.sapper.hcdc.core.schema;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.SchemaEntity;
import ai.sapper.hcdc.common.schema.SchemaVersion;
import ai.sapper.hcdc.common.utils.PathUtils;
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

import java.util.List;

@Getter
@Accessors(fluent = true)
public class SchemaManager {
    private SchemaManagerConfig config;
    private ZookeeperConnection zkConnection;

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
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public String save(@NonNull Schema schema, @NonNull SchemaEntity schemaEntity) throws Exception {
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

                }
            }
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
            public static final String __CONFIG_PATH = "managers.schema";
            public static final String CONFIG_CONNECTION = "connection";
            public static final String CONFIG_BASE_PATH = "path";
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
