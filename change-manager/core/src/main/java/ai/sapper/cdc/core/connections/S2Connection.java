package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import com.singlestore.jdbc.SingleStorePoolDataSource;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class S2Connection implements Connection {
    private S2ConnectionConfig config;
    private SingleStorePoolDataSource poolDataSource;
    private Object settings;

    @Override
    public String name() {
        return null;
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        return null;
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path) throws ConnectionError {
        return null;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings) throws ConnectionError {
        return null;
    }

    @Override
    public Connection connect() throws ConnectionError {
        return null;
    }

    @Override
    public Throwable error() {
        return null;
    }

    @Override
    public EConnectionState connectionState() {
        return null;
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public String path() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Getter
    @Accessors(fluent = true)
    public static class S2ConnectionConfig extends ConfigReader {
        private String serverUrl;
        private String dbName;
        private String user;
        private String password;
        private int poolSize = 32;

        public S2ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                  @NonNull String path) {
            super(config, path);
        }
    }
}
