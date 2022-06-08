package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import ai.sapper.hcdc.core.connections.state.DFSFileState;
import ai.sapper.hcdc.core.connections.state.StateManagerError;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Getter
@Accessors(fluent = true)
public class ZkStateManager {
    public static class Constants {
        public static final String ZK_PATH_SUFFIX = "/hcdc/agent/namenode";
    }

    @Getter
    @Setter
    public static class NameNodeAgentState {
        private String namespace;
        private long updatedTime;
        private long lastTxId;
    }

    private ZookeeperConnection connection;
    private ZkStateManagerConfig config;
    private String zkPath;
    private NameNodeAgentState agentState;
    private ObjectMapper mapper = new ObjectMapper();

    public ZkStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                               @NonNull ConnectionManager manger, @NonNull String namespace) throws StateManagerError {
        try {
            config = new ZkStateManagerConfig(xmlConfig);
            config.read();

            connection = manger.getConnection(config.zkConnection, ZookeeperConnection.class);
            CuratorFramework client = connection().client();
            if (!connection.isConnected()) {
                throw new StateManagerError("Error initializing ZooKeeper connection.");
            }
            zkPath = String.format("%s%s%s", basePath(), Constants.ZK_PATH_SUFFIX, namespace);
            if (client.checkExists().forPath(zkPath) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkPath);
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
                agentState = new NameNodeAgentState();
                agentState.namespace = namespace;
                agentState.lastTxId = 0;
                agentState.updatedTime = 0;

                String json = mapper.writeValueAsString(agentState);
                client.setData().forPath(zkPath, json.getBytes(StandardCharsets.UTF_8));
            } else {
                byte[] data = client.getData().forPath(zkPath);
                if (data == null || data.length <= 0) {
                    throw new StateManagerError(String.format("ZooKeeper state data corrupted. [path=%s]", zkPath));
                }
                String json = new String(data);
                agentState = mapper.readValue(json, NameNodeAgentState.class);
                if (agentState.namespace.compareTo(namespace) != 0) {
                    throw new StateManagerError(String.format("Invalid state data: namespace mismatch. [expected=%s][actual=%s]", namespace, agentState.namespace));
                }
            }
            return this;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public NameNodeAgentState update(long txId) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        Preconditions.checkArgument(txId > agentState.lastTxId);

        synchronized (this) {
            try {
                CuratorFramework client = connection().client();

                agentState.lastTxId = txId;
                agentState.updatedTime = System.currentTimeMillis();

                String json = mapper.writeValueAsString(agentState);
                client.setData().forPath(zkPath, json.getBytes(StandardCharsets.UTF_8));

                return agentState;
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    public String basePath() {
        return config().basePath();
    }

    @Getter
    @Accessors(fluent = true)
    public static class ZkStateManagerConfig extends ConfigReader {
        private static final class Constants {
            private static final String CONFIG_ZK_BASE = "basePath";
            private static final String CONFIG_ZK_CONNECTION = "connection";
        }

        private static final String __CONFIG_PATH = "state.manager";

        private String basePath;
        private String zkConnection;

        public ZkStateManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
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
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing State Manager configuration.", t);
            }
        }
    }
}
