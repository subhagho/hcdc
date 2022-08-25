package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.ConfigReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
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

    /**
     * @return
     */
    @Override
    public String name() {
        return config.name();
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                config = new HdfsConfig(xmlConfig);
                config.read();

                hdfsConfig = new Configuration();
                hdfsConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getCanonicalName());
                hdfsConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getCanonicalName());
                hdfsConfig.set(HDFS_PARAM_DEFAULT_FS, config.primaryNameNodeUri);
                hdfsConfig.set(HDFS_PARAM_DFS_IMPLEMENTATION, DistributedFileSystem.class.getName());
                if (config.isSecurityEnabled) {
                    enableSecurity(hdfsConfig);
                }
                state.state(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
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
        synchronized (state) {
            if (!state.isConnected()
                    && (state.state() == EConnectionState.Initialized || state.state() == EConnectionState.Closed)) {
                state.clear(EConnectionState.Initialized);
                try {
                    fileSystem = FileSystem.get(hdfsConfig);
                    if (config.isAdminEnabled) {
                        adminClient = new HdfsAdmin(URI.create(config.primaryNameNodeUri), hdfsConfig);
                    }
                    if (config.parameters != null && !config.parameters.isEmpty()) {
                        for (String key : config.parameters.keySet()) {
                            hdfsConfig.set(key, config.parameters.get(key));
                        }
                    }
                    dfsClient = new DFSClient(URI.create(config.primaryNameNodeUri), hdfsConfig);
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

    /**
     * @return
     */
    @Override
    public HierarchicalConfiguration<ImmutableNode> config() {
        return config.get();
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
    public static class HdfsConfig extends ConfigReader {
        private static final class Constants {
            private static final String CONN_NAME = "name";
            private static final String CONN_PRI_NAME_NODE_URI = "namenode.primary.URI";
            private static final String CONN_SEC_NAME_NODE_URI = "namenode.secondary.URI";
            private static final String CONN_SECURITY_ENABLED = "security.enabled";
            private static final String CONN_ADMIN_CLIENT_ENABLED = "enableAdmin";
        }

        private static final String __CONFIG_PATH = "hdfs";

        private String name;
        private String primaryNameNodeUri;
        private String secondaryNameNodeUri;
        private boolean isSecurityEnabled = false;
        private boolean isAdminEnabled = false;

        private Map<String, String> parameters;

        public HdfsConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("HDFS Configuration not set or is NULL");
            }
            try {
                name = get().getString(Constants.CONN_NAME);
                if (Strings.isNullOrEmpty(name)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.CONN_NAME));
                }
                primaryNameNodeUri = get().getString(Constants.CONN_PRI_NAME_NODE_URI);
                if (Strings.isNullOrEmpty(primaryNameNodeUri)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.CONN_PRI_NAME_NODE_URI));
                }
                if (get().containsKey(Constants.CONN_SEC_NAME_NODE_URI)) {
                    secondaryNameNodeUri = get().getString(Constants.CONN_SEC_NAME_NODE_URI);
                    if (Strings.isNullOrEmpty(secondaryNameNodeUri)) {
                        throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.CONN_SEC_NAME_NODE_URI));
                    }
                }
                if (checkIfNodeExists((String) null, Constants.CONN_SECURITY_ENABLED))
                    isSecurityEnabled = get().getBoolean(Constants.CONN_SECURITY_ENABLED);
                if (checkIfNodeExists((String) null, Constants.CONN_ADMIN_CLIENT_ENABLED))
                    isAdminEnabled = get().getBoolean(Constants.CONN_ADMIN_CLIENT_ENABLED);

                parameters = readParameters();
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
