package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.sapper.hcdc.common.DefaultLogger.stacktrace;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectionManager {
    public static Logger __LOG = LoggerFactory.getLogger(ConnectionManager.class);
    private static final String __PATH = "connections";
    private static final String __CONNECTION_LIST = "connection";
    private static final String __TYPE = "type";

    private String configPath;
    private HierarchicalConfiguration<ImmutableNode> config;
    private Map<String, Connection> connections = new HashMap<>();

    public ConnectionManager init(@NonNull HierarchicalConfiguration<ImmutableNode> config, String pathPrefix) throws ConnectionError {
        if (Strings.isNullOrEmpty(pathPrefix)) {
            configPath = __PATH;
        } else {
            configPath = String.format("%s.%s", pathPrefix, __PATH);
        }
        try {
            this.config = config.configurationAt(configPath);
            int count = initConnections();
            __LOG.info(String.format("Initialized %d connections...", count));
            return this;
        } catch (Exception ex) {
            connections.clear();
            stacktrace(__LOG, ex);
            throw new ConnectionError("Error Initializing connections.", ex);
        }
    }

    private int initConnections() throws Exception {
        if (ConfigReader.checkIfNodeExists(config, __CONNECTION_LIST)) {
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(__CONNECTION_LIST);
            if (!nodes.isEmpty()) {
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    Connection connection = initConnection(node);
                    __LOG.info(String.format("Initialized connection: [name=%s][type=%s]...", connection.name(), connection.getClass().getCanonicalName()));
                }
            }
        }
        return connections.size();
    }

    private Connection initConnection(HierarchicalConfiguration<ImmutableNode> node) throws Exception {
        String type = node.getString(__TYPE);
        if (Strings.isNullOrEmpty(type)) {
            throw new ConnectionError(String.format("Connection type not found. [node=%s]", node.toString()));
        }
        Class<? extends Connection> cls = (Class<? extends Connection>) Class.forName(type);
        Connection connection = cls.newInstance();
        connection.init(node);
        Preconditions.checkState(!Strings.isNullOrEmpty(connection.name()));
        Preconditions.checkState(connection.state() == Connection.EConnectionState.Initialized);

        connections.put(connection.name(), connection);
        return connection;
    }

    public Connection getConnection(@NonNull String name) {
        return connections.get(name);
    }

    public <T extends Connection> T getConnection(@NonNull String name, @NonNull Class<? extends Connection> type) {
        Connection connection = getConnection(name);
        if (connection != null && connection.getClass().equals(type)) {
            return (T) connection;
        }
        return null;
    }
}
