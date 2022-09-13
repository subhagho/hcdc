package ai.sapper.cdc.core;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.keystore.KeyStore;
import ai.sapper.cdc.core.utils.DistributedLockBuilder;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class BaseEnv {
    public static class Constants {
        public static final String CONFIG_ENV = "env";
        public static final String CONFIG_ENV_NAME = String.format("%s.name", CONFIG_ENV);
    }

    private ConnectionManager connectionManager;
    private String storeKey;
    private KeyStore keyStore;
    private final DistributedLockBuilder lockBuilder = new DistributedLockBuilder();
    private String environment;
    private HierarchicalConfiguration<ImmutableNode> rootConfig;
    private List<ExitCallback> exitCallbacks;

    public BaseEnv withStoreKey(@NonNull String storeKey) {
        this.storeKey = storeKey;
        return this;
    }

    public BaseEnv init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        String temp = System.getProperty("java.io.tmpdir");
        temp = String.format("%s/sapper/cdc/%s", temp, getClass().getSimpleName());
        File tdir = new File(temp);
        if (!tdir.exists()) {
            tdir.mkdirs();
        }
        System.setProperty("java.io.tmpdir", temp);
        environment = xmlConfig.getString(Constants.CONFIG_ENV_NAME);
        if (Strings.isNullOrEmpty(environment)) {
            throw new ConfigurationException(
                    String.format("Base Env: missing parameter. [name=%s]", Constants.CONFIG_ENV_NAME));
        }
        rootConfig = xmlConfig.configurationAt(Constants.CONFIG_ENV);

        return this;
    }

    public void setup(@NonNull String module,
                      @NonNull String connectionsConfigPath) throws ConfigurationException {
        try {

            if (ConfigReader.checkIfNodeExists(rootConfig, KeyStore.__CONFIG_PATH)) {
                String c = rootConfig.getString(KeyStore.CONFIG_KEYSTORE_CLASS);
                if (Strings.isNullOrEmpty(c)) {
                    throw new ConfigurationException(
                            String.format("Key Store class not defined. [config=%s]", KeyStore.CONFIG_KEYSTORE_CLASS));
                }
                Class<? extends KeyStore> cls = (Class<? extends KeyStore>) Class.forName(c);
                keyStore = cls.getDeclaredConstructor().newInstance();
                keyStore.withPassword(storeKey)
                        .init(rootConfig);
            }
            this.storeKey = null;

            connectionManager = new ConnectionManager()
                    .withKeyStore(keyStore)
                    .withEnv(environment);
            connectionManager.init(rootConfig, connectionsConfigPath);

            lockBuilder.withEnv(environment)
                    .init(rootConfig, module, connectionManager);

        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public synchronized BaseEnv addExitCallback(@NonNull ExitCallback callback) {
        if (exitCallbacks == null) {
            exitCallbacks = new ArrayList<>();
        }
        exitCallbacks.add(callback);
        return this;
    }

    public DistributedLock createLock(@NonNull String path,
                                      @NonNull String module,
                                      @NonNull String name) throws Exception {
        return lockBuilder.createLock(path, module, name);
    }

    public void saveLocks() throws Exception {
        lockBuilder.save();
    }

    public void close() throws Exception {
        if (connectionManager != null) {
            connectionManager.close();
        }
    }
}
