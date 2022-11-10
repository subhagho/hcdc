package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
public class JavaKeyStoreUtil {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    @Parameter(names = {"--key", "-k"}, required = true, description = "Key Name (Alias)")
    private String key;
    @Parameter(names = {"--value", "-v"}, required = true, description = "Value to store.")
    private String value;
    @Parameter(names = {"--passwd", "-p"}, required = true, description = "Key Store password.")
    private String password;
    private EConfigFileType fileSource = EConfigFileType.File;

    public void run() throws Exception {
        if (!Strings.isNullOrEmpty(configSource)) {
            fileSource = EConfigFileType.parse(configSource);
        }
        Preconditions.checkNotNull(fileSource);
        HierarchicalConfiguration<ImmutableNode> config = ConfigReader.read(configFile, fileSource);
        String c = config.getString(KeyStore.CONFIG_KEYSTORE_CLASS);
        if (Strings.isNullOrEmpty(c)) {
            throw new ConfigurationException(
                    String.format("Key Store class not defined. [config=%s]", KeyStore.CONFIG_KEYSTORE_CLASS));
        }
        Class<? extends KeyStore> cls = (Class<? extends KeyStore>) Class.forName(c);
        KeyStore keyStore = cls.getDeclaredConstructor().newInstance();
        keyStore.withPassword(password)
                .init(config);
        keyStore.save(key, value);
        keyStore.flush();
    }

    public static void main(String[] args) {
        try {
            JavaKeyStoreUtil util = new JavaKeyStoreUtil();
            JCommander.newBuilder().addObject(util).build().parse(args);
            util.run();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.LOGGER.error(t.getLocalizedMessage());
            t.printStackTrace();
        }
    }
}
