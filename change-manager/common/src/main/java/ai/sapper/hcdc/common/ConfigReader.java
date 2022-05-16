package ai.sapper.hcdc.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class ConfigReader {
    private final XMLConfiguration config;
    private final String path;

    public ConfigReader(@NonNull XMLConfiguration config, @NonNull String path) {
        this.config = config;
        this.path = path;
    }

    public ConfigReader(@NonNull XMLConfiguration config, @NonNull String path, String pathPrefix) {
        this.config = config;
        if (!Strings.isNullOrEmpty(pathPrefix)) {
            this.path = String.format("%s.%s", pathPrefix, path);
        } else
            this.path = path;
    }

    public HierarchicalConfiguration<ImmutableNode> get() {
        return config.configurationAt(path);
    }

    public HierarchicalConfiguration<ImmutableNode> get(@NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        String key = String.format("%s.%s", path, name);
        return config.configurationAt(key);
    }

    public List<HierarchicalConfiguration<ImmutableNode>> getCollection(@NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        String key = String.format("%s.%s", path, name);
        return config.configurationsAt(key);
    }

    public static XMLConfiguration read(@NonNull String filename) throws ConfigurationException {
        File cf = new File(filename);
        if (!cf.exists()) {
            throw new ConfigurationException(String.format("Specified configuration file not found. [file=%s]", cf.getAbsolutePath()));
        }
        if (!cf.canRead()) {
            throw new ConfigurationException(String.format("Cannot read configuration file. [file=%s]", cf.getAbsolutePath()));
        }
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder =
                new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
                        .configure(params.xml()
                                .setFileName(cf.getAbsolutePath()));
        return builder.getConfiguration();
    }
}
