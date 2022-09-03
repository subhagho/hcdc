package ai.sapper.cdc.core;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.keystore.KeyStore;
import ai.sapper.cdc.core.utils.DistributedLockBuilder;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class BaseEnv {
    public static class Constants {
        public static final String CONFIG_ENV = "env";
    }

    private ConnectionManager connectionManager;
    private String storeKey;
    private KeyStore keyStore;
    private final DistributedLockBuilder lockBuilder = new DistributedLockBuilder();
    private String environment;

    public BaseEnv withStoreKey(@NonNull String storeKey) {
        this.storeKey = storeKey;
        return this;
    }

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                     @NonNull String module,
                     @NonNull String connectionsConfigPath) throws ConfigurationException {
        try {
            environment = xmlConfig.getString(Constants.CONFIG_ENV);
            if (Strings.isNullOrEmpty(environment)) {
                throw new ConfigurationException(
                        String.format("Base Env: missing parameter. [name=%s]", Constants.CONFIG_ENV));
            }
            if (ConfigReader.checkIfNodeExists(xmlConfig, KeyStore.__CONFIG_PATH)) {
                String c = xmlConfig.getString(KeyStore.CONFIG_KEYSTORE_CLASS);
                if (Strings.isNullOrEmpty(c)) {
                    throw new ConfigurationException(
                            String.format("Key Store class not defined. [config=%s]", KeyStore.CONFIG_KEYSTORE_CLASS));
                }
                Class<? extends KeyStore> cls = (Class<? extends KeyStore>) Class.forName(c);
                keyStore = cls.getDeclaredConstructor().newInstance();
                keyStore.withPassword(storeKey)
                        .init(xmlConfig);
            }
            this.storeKey = null;

            connectionManager = new ConnectionManager()
                    .withKeyStore(keyStore)
                    .withEnv(environment);
            connectionManager.init(xmlConfig, connectionsConfigPath);

            lockBuilder.withEnv(environment)
                    .init(xmlConfig, module, connectionManager);

        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
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
