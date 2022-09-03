package ai.sapper.cdc.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Enumeration;

public class JavaKeyStore extends KeyStore {
    public static final String CONFIG_KEYSTORE_FILE = "path";
    public static final String CONFIG_CIPHER_ALGO = "cipher.algo";
    public static final String CONFIG_KEYSTORE_TYPE = "type";
    private static final String KEYSTORE_TYPE = "JCEKS";


    private java.security.KeyStore store;
    private String keyStoreFile;
    private HierarchicalConfiguration<ImmutableNode> config;
    private String cipherAlgo = CIPHER_TYPE;
    private String keyStoreType = KEYSTORE_TYPE;

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
            String s = config.getString(CONFIG_CIPHER_ALGO);
            if (!Strings.isNullOrEmpty(s)) {
                cipherAlgo = s;
            }
            s = config.getString(CONFIG_KEYSTORE_TYPE);
            if (!Strings.isNullOrEmpty(s)) {
                keyStoreType = s;
            }

            File kf = new File(keyStoreFile);
            if (!kf.exists()) {
                createEmptyStore(kf.getAbsolutePath(), password);
            } else {
                store = java.security.KeyStore.getInstance(keyStoreType);
                store.load(new FileInputStream(kf), password.toCharArray());
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void createEmptyStore(String path, String password) throws Exception {
        store = java.security.KeyStore.getInstance(keyStoreType);
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
                = new java.security.KeyStore.SecretKeyEntry(generate(value, cipherAlgo));
        java.security.KeyStore.ProtectionParameter parameter
                = new java.security.KeyStore.PasswordProtection(password.toCharArray());
        store.setEntry(name, secret, parameter);

    }

    @Override
    public String read(@NonNull String name,
                       @NonNull String password) throws Exception {
        Preconditions.checkNotNull(store);
        java.security.KeyStore.ProtectionParameter param
                = new java.security.KeyStore.PasswordProtection(password.toCharArray());
        if (store.containsAlias(name)) {
            java.security.KeyStore.SecretKeyEntry key
                    = (java.security.KeyStore.SecretKeyEntry) store.getEntry(name, param);
            return extractValue(key.getSecretKey(), cipherAlgo);
        }
        return null;
    }

    @Override
    public void delete(@NonNull String name) throws Exception {
        Preconditions.checkNotNull(store);
        store.deleteEntry(name);
    }

    @Override
    public void delete() throws Exception {
        Preconditions.checkNotNull(store);
        Enumeration<String> aliases = store.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            store.deleteEntry(alias);
        }
        store = null;

        Files.delete(Paths.get(keyStoreFile));
    }

    @Override
    public String flush(@NonNull String password) throws Exception {
        Preconditions.checkNotNull(store);
        File file = new File(keyStoreFile);
        store.store(new FileOutputStream(file), password.toCharArray());
        return file.getAbsolutePath();
    }
}
