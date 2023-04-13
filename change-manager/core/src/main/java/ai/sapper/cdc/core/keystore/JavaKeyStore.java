package ai.sapper.cdc.core.keystore;

import ai.sapper.cdc.common.utils.ChecksumUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;


public class JavaKeyStore extends KeyStore {
    public static final String CONFIG_KEYSTORE_FILE = "path";
    public static final String CONFIG_CIPHER_ALGO = "cipher.algo";
    public static final String CONFIG_KEYSTORE_TYPE = "type";
    private static final String KEYSTORE_TYPE = "PKCS12";


    private java.security.KeyStore store;
    private String keyStoreFile;
    private HierarchicalConfiguration<ImmutableNode> config;
    private String cipherAlgo = CIPHER_TYPE;
    private String keyStoreType = KEYSTORE_TYPE;
    private String passwdHash;

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
                try (FileInputStream fis = new FileInputStream(kf)) {
                    store.load(fis, password.toCharArray());
                }
            }
            keyStoreFile = kf.getAbsolutePath();
            passwdHash = ChecksumUtils.generateHash(password);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public File filePath(@NonNull String key) throws Exception {
        if (!authenticate(key)) {
            throw new Exception("Authentication failed...");
        }
        File file = new File(keyStoreFile);
        if (file.exists()) {
            return file;
        }
        throw new Exception("KeyStore file not found.");
    }

    private void createEmptyStore(String path, String password) throws Exception {
        store = java.security.KeyStore.getInstance(keyStoreType);
        store.load(null, password.toCharArray());

        // Save the keyStore
        try (FileOutputStream fos = new FileOutputStream(path)) {
            store.store(fos, password.toCharArray());
        }
        save(DEFAULT_KEY, password);
        flush(password);
    }

    @Override
    public void save(@NonNull String name,
                     @NonNull String value,
                     @NonNull String password) throws Exception {
        Preconditions.checkNotNull(store);
        String hash = ChecksumUtils.generateHash(password);
        if (!Strings.isNullOrEmpty(passwdHash))
            Preconditions.checkArgument(hash.equals(passwdHash));
        java.security.KeyStore.SecretKeyEntry secret
                = new java.security.KeyStore.SecretKeyEntry(generate(value, cipherAlgo));
        java.security.KeyStore.ProtectionParameter parameter
                = new java.security.KeyStore.PasswordProtection(password.toCharArray());
        store.setEntry(name, secret, parameter);
    }

    @Override
    public String read(@NonNull String name,
                       @NonNull String password) throws Exception {
        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
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
        try (FileOutputStream fos = new FileOutputStream(keyStoreFile, false)) {
            store.store(fos, password.toCharArray());
        }
        return keyStoreFile;
    }
}
