package ai.sapper.cdc.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.Key;

public class JavaKeyStore extends KeyStore {
    public static final String __CONFIG_PATH = "keystore";
    public static final String CONFIG_KEYSTORE_FILE = "path";
    public static final String CONFIG_CRYPTO_TYPE = "type";

    private java.security.KeyStore store;
    private String keyStoreFile;
    private HierarchicalConfiguration<ImmutableNode> config;
    private String cryptoType = "AES";
    private SecretKey secretKey;

    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                     @NonNull String password) throws ConfigurationException {
        try {
            config = configNode.configurationAt(__CONFIG_PATH);
            Preconditions.checkNotNull(config);
            keyStoreFile = config.getString(CONFIG_KEYSTORE_FILE);
            if (Strings.isNullOrEmpty(keyStoreFile)) {
                throw new ConfigurationException(
                        String.format("Java Key Store: missing keystore file path. [config=%s]",
                                CONFIG_KEYSTORE_FILE));
            }
            String s = config.getString(CONFIG_CRYPTO_TYPE);
            if (!Strings.isNullOrEmpty(s)) {
                cryptoType = s;
            }
            secretKey = KeyGenerator.getInstance(cryptoType).generateKey();

            File kf = new File(keyStoreFile);
            if (!kf.exists()) {
                createEmptyStore(kf.getAbsolutePath(), password);
            } else {
                store = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
                store.load(new FileInputStream(kf), password.toCharArray());
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void createEmptyStore(String path, String password) throws Exception {
        store = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        store.load(null, password.toCharArray());

        // Save the keyStore
        FileOutputStream fos = new FileOutputStream(path);
        store.store(fos, password.toCharArray());
        fos.close();
    }

    @Override
    public void save(@NonNull String name,
                     @NonNull String value,
                     @NonNull String password) throws Exception {
        Preconditions.checkNotNull(store);
        java.security.KeyStore.SecretKeyEntry secret
                = new java.security.KeyStore.SecretKeyEntry(secretKey);
        java.security.KeyStore.ProtectionParameter parameter
                = new java.security.KeyStore.PasswordProtection(value.toCharArray());
        store.setEntry(name, secret, parameter);
    }

    @Override
    public String read(@NonNull String name,
                       @NonNull String password) throws Exception {
        Preconditions.checkNotNull(store);
        java.security.KeyStore.ProtectionParameter param
                = new java.security.KeyStore.PasswordProtection(password.toCharArray());
        java.security.KeyStore.Entry key = store.getEntry(name, param);
        return key.toString();
    }
}
