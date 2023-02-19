package ai.sapper.cdc.core.connections.hadoop;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.connections.settngs.HdfsConnectionSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class HdfsConnection implements Connection {
    private static final String HDFS_PARAM_DEFAULT_FS = "fs.defaultFS";
    private static final String HDFS_PARAM_DFS_IMPLEMENTATION = "fs.hdfs.impl";

    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    private HdfsConfig config;
    protected Configuration hdfsConfig = null;
    protected FileSystem fileSystem;
    protected HdfsAdmin adminClient;
    protected DFSClient dfsClient;

    protected HdfsConnectionSettings.HdfsBaseSettings settings;

    /**
     * @return
     */
    @Override
    public String name() {
        return settings().getName();
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                config = new HdfsConfig(xmlConfig);
                settings = config.read();

                setupHadoopConfig();

                state.state(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }

    private void setupHadoopConfig() throws Exception {
        Preconditions.checkState(settings instanceof HdfsConnectionSettings.HdfsSettings);
        hdfsConfig = new Configuration();
        hdfsConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getCanonicalName());
        hdfsConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getCanonicalName());
        hdfsConfig.set(HDFS_PARAM_DEFAULT_FS, ((HdfsConnectionSettings.HdfsSettings) settings).getPrimaryNameNodeUri());
        hdfsConfig.set(HDFS_PARAM_DFS_IMPLEMENTATION, DistributedFileSystem.class.getName());
        if (settings.isSecurityEnabled()) {
            enableSecurity(hdfsConfig);
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);

                CuratorFramework client = connection.client();
                String hpath = new PathUtils.ZkPathBuilder(path)
                        .withPath(HdfsConfig.__CONFIG_PATH)
                        .build();
                if (client.checkExists().forPath(hpath) == null) {
                    throw new Exception(String.format("HDFS Settings path not found. [path=%s]", hpath));
                }
                byte[] data = client.getData().forPath(hpath);
                settings = JSONUtils.read(data, HdfsConnectionSettings.HdfsSettings.class);
                Preconditions.checkNotNull(settings);
                Preconditions.checkState(name.equals(settings.getName()));
                setupHadoopConfig();

                state.state(EConnectionState.Initialized);
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof HdfsConnectionSettings.HdfsSettings);
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                this.settings = (HdfsConnectionSettings.HdfsBaseSettings) settings;
                setupHadoopConfig();
                state.clear(EConnectionState.Unknown);
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    protected void enableSecurity(Configuration conf) throws Exception {
        HdfsSecurityConfig sConfig = new HdfsSecurityConfig(config.config());
        sConfig.read();
        sConfig.setup(conf);
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkState(settings instanceof HdfsConnectionSettings.HdfsSettings);
        synchronized (state) {
            if (!state.isConnected()
                    && (state.state() == EConnectionState.Initialized || state.state() == EConnectionState.Closed)) {
                state.clear(EConnectionState.Initialized);
                try {
                    fileSystem = FileSystem.get(hdfsConfig);
                    if (settings.isAdminEnabled()) {
                        adminClient = new HdfsAdmin(URI.create(
                                ((HdfsConnectionSettings.HdfsSettings) settings).getPrimaryNameNodeUri()), hdfsConfig);
                    }
                    if (settings.getParameters() != null && !settings.getParameters().isEmpty()) {
                        for (String key : settings.getParameters().keySet()) {
                            hdfsConfig.set(key, settings.getParameters().get(key));
                        }
                    }
                    dfsClient = new DFSClient(URI.create(
                            ((HdfsConnectionSettings.HdfsSettings) settings).getPrimaryNameNodeUri()), hdfsConfig);
                    state.state(EConnectionState.Connected);
                } catch (Throwable t) {
                    state.error(t);
                    throw new ConnectionError("Error opening HDFS connection.", t);
                }
            }
        }
        return this;
    }

    /**
     * @return
     */
    @Override
    public Throwable error() {
        return state.error();
    }

    /**
     * @return
     */
    @Override
    public EConnectionState connectionState() {
        return state.state();
    }

    /**
     * @return
     */
    @Override
    public boolean isConnected() {
        return state.isConnected();
    }

    @Override
    public String path() {
        return HdfsConfig.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return settings.getType();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (state.isConnected()) {
                state.state(EConnectionState.Closed);
            }
            try {
                if (fileSystem != null) {
                    fileSystem.close();
                    fileSystem = null;
                }
                if (adminClient != null) {
                    adminClient = null;
                }
            } catch (Exception ex) {
                state.error(ex);
                throw new IOException("Error closing HDFS connection.", ex);
            }
        }
    }

    public final FileSystem get() throws ConnectionError {
        if (!state.isConnected()) {
            throw new ConnectionError(String.format("HDFS Connection not available. [state=%s]", state.state().name()));
        }
        return fileSystem;
    }

    @Getter
    @Accessors(fluent = true)
    public static class HdfsConfig extends ConnectionConfig {
        public static final class Constants {
            public static final String CONN_PRI_NAME_NODE_URI = "namenode.primary.URI";
            public static final String CONN_SEC_NAME_NODE_URI = "namenode.secondary.URI";
            public static final String CONN_SECURITY_ENABLED = "security.enabled";
            public static final String CONN_ADMIN_CLIENT_ENABLED = "enableAdmin";
        }

        private static final String __CONFIG_PATH = "hdfs";

        protected final HdfsConnectionSettings.HdfsBaseSettings settings;

        public HdfsConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
            settings = new HdfsConnectionSettings.HdfsSettings();
        }

        public HdfsConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                          @NonNull String path,
                          @NonNull HdfsConnectionSettings.HdfsBaseSettings settings) {
            super(config, path);
            this.settings = settings;
        }

        public HdfsConnectionSettings.HdfsBaseSettings read() throws ConfigurationException {
            Preconditions.checkNotNull(config());
            if (get() == null) {
                throw new ConfigurationException("HDFS Configuration not set or is NULL");
            }
            try {
                HdfsConnectionSettings.HdfsSettings settings = (HdfsConnectionSettings.HdfsSettings) this.settings;
                settings.setName(get().getString(ConnectionConfig.CONFIG_NAME));
                settings.setPrimaryNameNodeUri(get().getString(Constants.CONN_PRI_NAME_NODE_URI));
                checkStringValue(settings.getPrimaryNameNodeUri(), getClass(), Constants.CONN_PRI_NAME_NODE_URI);
                if (get().containsKey(Constants.CONN_SEC_NAME_NODE_URI)) {
                    settings.setSecondaryNameNodeUri(get().getString(Constants.CONN_SEC_NAME_NODE_URI));
                    checkStringValue(settings.getSecondaryNameNodeUri(), getClass(), Constants.CONN_SEC_NAME_NODE_URI);
                }
                if (checkIfNodeExists((String) null, Constants.CONN_SECURITY_ENABLED))
                    settings.setSecurityEnabled(get().getBoolean(Constants.CONN_SECURITY_ENABLED));
                if (checkIfNodeExists((String) null, Constants.CONN_ADMIN_CLIENT_ENABLED))
                    settings.setAdminEnabled(get().getBoolean(Constants.CONN_ADMIN_CLIENT_ENABLED));

                settings.setParameters(readParameters());

                settings.validate();
                return settings;
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class HdfsSecurityConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "security";
        private static final String HDFS_PARAM_SECURITY_REQUIRED = "hadoop.security.authorization";
        private static final String HDFS_PARAM_SECURITY_MODE = "hadoop.security.authentication";
        private static final String HDFS_PARAM_SECURITY_KB_HOST = "java.security.krb5.kdc";
        private static final String HDFS_PARAM_SECURITY_KB_REALM = "java.security.krb5.realm";
        private static final String HDFS_PARAM_SECURITY_PRINCIPLE = "dfs.namenode.kerberos.principal.pattern";
        private static final String HDFS_SECURITY_TYPE = "kerberos";
        private static final String HDFS_SECURITY_USERNAME = "kerberos.username";
        private static final String HDFS_SECURITY_KEYTAB = "kerberos.user.keytab";

        private Map<String, String> params;

        public HdfsSecurityConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            params = readParameters();
        }

        public void setup(@NonNull Configuration conf) throws ConfigurationException, IOException {
            Preconditions.checkState(params != null);


            // set kerberos host and realm
            System.setProperty(HDFS_PARAM_SECURITY_KB_HOST, params.get(HDFS_PARAM_SECURITY_KB_HOST));
            System.setProperty(HDFS_PARAM_SECURITY_KB_REALM, params.get(HDFS_PARAM_SECURITY_KB_REALM));

            conf.set(HDFS_PARAM_SECURITY_MODE, HDFS_SECURITY_TYPE);
            conf.set(HDFS_PARAM_SECURITY_REQUIRED, "true");

            String principle = params.get(HDFS_PARAM_SECURITY_PRINCIPLE);
            if (!Strings.isNullOrEmpty(principle)) {
                conf.set(HDFS_PARAM_SECURITY_PRINCIPLE, principle);
            }
            String username = params.get(HDFS_SECURITY_USERNAME);
            if (Strings.isNullOrEmpty(username)) {
                throw new ConfigurationException(String.format("Missing Kerberos user. [param=%s]", HDFS_SECURITY_USERNAME));
            }
            String keyteabf = params.get(HDFS_SECURITY_KEYTAB);
            if (Strings.isNullOrEmpty(keyteabf)) {
                throw new ConfigurationException(String.format("Missing keytab path. [param=%s]", HDFS_SECURITY_KEYTAB));
            }
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(username, keyteabf);
        }
    }
}
