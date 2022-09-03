package ai.sapper.cdc.core.keystore;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.SecureRandom;

@Getter
@Accessors(fluent = true)
public abstract class KeyStore {
    public static final String __CONFIG_PATH = "keystore";
    public static final String CONFIG_KEYSTORE_CLASS = String.format("%s.class", __CONFIG_PATH);
    public static final String CIPHER_TYPE = "PBEWithMD5AndDES";
    private static final int DEFAULT_ITERATION_COUNT = 8;
    private static final int DEFAULT_KEY_LENGTH = 128;
    @Getter(AccessLevel.NONE)
    private SecretKey password;

    public KeyStore withPassword(@NonNull String password) throws Exception {
        this.password = generate(password, CIPHER_TYPE);
        return this;
    }

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode) throws ConfigurationException {
        try {
            init(configNode, extractValue(password, CIPHER_TYPE));
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public abstract void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                              @NonNull String password) throws ConfigurationException;

    public void save(@NonNull String name,
                     @NonNull String value) throws Exception {
        save(name, value, extractValue(password, CIPHER_TYPE));
    }

    public String read(@NonNull String name) throws Exception {
        return read(name, extractValue(password, CIPHER_TYPE));
    }

    public abstract void save(@NonNull String name,
                              @NonNull String value,
                              @NonNull String password) throws Exception;

    public abstract String read(@NonNull String name,
                                @NonNull String password) throws Exception;

    public abstract void delete(@NonNull String name) throws Exception;

    public abstract void delete() throws Exception;

    public String flush() throws Exception {
        return flush(extractValue(password, CIPHER_TYPE));
    }

    public abstract String flush(@NonNull String password) throws Exception;


    public SecretKey generate(@NonNull String value,
                              @NonNull String cipherAlgo) throws Exception {
        SecureRandom rand = new SecureRandom();
        byte[] salt = new byte[32];
        rand.nextBytes(salt);
        char[] buffer = value.toCharArray();
        PBEKeySpec spec = new PBEKeySpec(buffer,
                salt,
                DEFAULT_ITERATION_COUNT,
                DEFAULT_KEY_LENGTH);
        SecretKeyFactory factory = SecretKeyFactory.getInstance(cipherAlgo);
        return factory.generateSecret(spec);
    }

    public String extractValue(@NonNull SecretKey secretKey,
                               @NonNull String cipherAlgo) throws Exception {
        SecretKeyFactory factory = SecretKeyFactory.getInstance(cipherAlgo);
        PBEKeySpec spec = (PBEKeySpec) factory.getKeySpec(secretKey, PBEKeySpec.class);
        return new String(spec.getPassword());
    }
}
