package ai.sapper.cdc.common;

import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.commons.configuration2.io.ClasspathLocationStrategy;
import org.apache.commons.configuration2.io.CombinedLocationStrategy;
import org.apache.commons.configuration2.io.FileLocationStrategy;
import org.apache.commons.configuration2.io.ProvidedURLLocationStrategy;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.*;

@Getter
@Setter
@Accessors(fluent = true)
public class ConfigReader {
    public static final String __NODE_PARAMETERS = "parameters";
    public static final String __NODE_PARAMETER = "parameter";
    public static final String __PARAM_NAME = "name";
    public static final String __PARAM_VALUE = "value";
    public static final String CONFIG_PARAMS = "parameters";

    private final HierarchicalConfiguration<ImmutableNode> config;

    public ConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
        this.config = config.configurationAt(path);
    }

    public void read(@NonNull Class<? extends ConfigReader> type) throws ConfigurationException {
        try {
            Field[] fields = ReflectionUtils.getAllFields(getClass());
            if (fields != null) {
                for (Field field : fields) {
                    if (field.isAnnotationPresent(Config.class)) {
                        Config c = field.getAnnotation(Config.class);
                        if (c.name().compareTo(CONFIG_PARAMS) == 0) {
                            Map<String, String> params = readParameters();
                            ReflectionUtils.setValue(params, this, field);
                            continue;
                        }
                        if (checkIfNodeExists(config, c.name())) {
                            if (c.type().equals(String.class)) {
                                ReflectionUtils.setValue(config.getString(c.name()), this, field);
                            } else if (ReflectionUtils.isBoolean(c.type())) {
                                ReflectionUtils.setValue(config.getBoolean(c.name()), this, field);
                            } else if (ReflectionUtils.isShort(c.type())) {
                                ReflectionUtils.setValue(config.getShort(c.name()), this, field);
                            } else if (ReflectionUtils.isInt(c.type())) {
                                ReflectionUtils.setValue(config.getInt(c.name()), this, field);
                            } else if (ReflectionUtils.isLong(c.type())) {
                                ReflectionUtils.setValue(config.getLong(c.name()), this, field);
                            } else if (ReflectionUtils.isFloat(c.type())) {
                                ReflectionUtils.setValue(config.getFloat(c.name()), this, field);
                            } else if (ReflectionUtils.isDouble(c.type())) {
                                ReflectionUtils.setValue(config.getDouble(c.name()), this, field);
                            }
                        } else if (c.required()) {
                            throw new ConfigurationException(String.format("Required configuration not found. [name=%s]", c.name()));
                        }
                    }
                }
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public boolean checkIfNodeExists(String path, @NonNull String name) {
        String key = name;
        if (!Strings.isNullOrEmpty(path)) {
            key = String.format("%s.%s", path, key);
        }
        if (!Strings.isNullOrEmpty(key)) {
            try {
                List<HierarchicalConfiguration<ImmutableNode>> nodes = get().configurationsAt(name);
                if (nodes != null) return !nodes.isEmpty();
            } catch (ConfigurationRuntimeException e) {
                // Ignore Exception
            }
        }
        return false;
    }

    public HierarchicalConfiguration<ImmutableNode> get() {
        return config;
    }

    public HierarchicalConfiguration<ImmutableNode> get(@NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (checkIfNodeExists((String) null, name))
            return config.configurationAt(name);
        return null;
    }

    public List<HierarchicalConfiguration<ImmutableNode>> getCollection(@NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (checkIfNodeExists((String) null, name))
            return config.configurationsAt(name);
        return null;
    }

    protected Map<String, String> readParameters() throws ConfigurationException {
        if (checkIfNodeExists((String) null, __NODE_PARAMETERS)) {
            HierarchicalConfiguration<ImmutableNode> pc = config.configurationAt(__NODE_PARAMETERS);
            if (pc != null) {
                List<HierarchicalConfiguration<ImmutableNode>> pl = pc.configurationsAt(__NODE_PARAMETER);
                if (pl != null && !pl.isEmpty()) {
                    Map<String, String> params = new HashMap<>(pl.size());
                    for (HierarchicalConfiguration<ImmutableNode> p : pl) {
                        String name = p.getString(__PARAM_NAME);
                        if (!Strings.isNullOrEmpty(name)) {
                            String value = p.getString(__PARAM_VALUE);
                            params.put(name, value);
                        }
                    }
                    return params;
                }
            }
        }
        return null;
    }

    public static XMLConfiguration readFromFile(@NonNull String filename) throws ConfigurationException {
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

    public static XMLConfiguration readFromClasspath(@NonNull String path) throws ConfigurationException {
        List<FileLocationStrategy> subs = Collections.singletonList(
                new ClasspathLocationStrategy());
        FileLocationStrategy strategy = new CombinedLocationStrategy(subs);
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder
                = new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
                .configure(params
                        .xml()
                        .setLocationStrategy(strategy)
                        .setURL(
                                ConfigReader.class.getClassLoader().getResource(path)
                        ));

        return builder.getConfiguration();
    }

    public static XMLConfiguration readFromURI(@NonNull String path) throws ConfigurationException {
        try {
            List<FileLocationStrategy> subs = Collections.singletonList(
                    new ProvidedURLLocationStrategy());
            FileLocationStrategy strategy = new CombinedLocationStrategy(subs);
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<XMLConfiguration> builder
                    = new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
                    .configure(params
                            .xml()
                            .setLocationStrategy(strategy)
                            .setURL(
                                    new URI(path).toURL()
                            ));
            return builder.getConfiguration();
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public static XMLConfiguration read(@NonNull String path, @NonNull EConfigFileType type) throws ConfigurationException {
        switch (type) {
            case File:
                return readFromFile(path);
            case Remote:
                return readFromURI(path);
            case Resource:
                return readFromClasspath(path);
        }
        throw new ConfigurationException(String.format("Invalid Config File type. [type=%s]", type.name()));
    }

    public static boolean checkIfNodeExists(@NonNull HierarchicalConfiguration<ImmutableNode> node, @NonNull String name) {
        if (!Strings.isNullOrEmpty(name)) {
            try {
                List<HierarchicalConfiguration<ImmutableNode>> nodes = node.configurationsAt(name);
                if (nodes != null) return !nodes.isEmpty();
            } catch (ConfigurationRuntimeException e) {
                // Ignore Exception
            }
        }
        return false;
    }

    public static <T> Map<String, T> readAsMap(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                               @NonNull String key) {
        Map<String, T> map = new LinkedHashMap<>();
        Configuration subset = config.subset(key);
        if (!subset.isEmpty()) {
            Iterator<String> it = subset.getKeys();
            while (it.hasNext()) {
                String k = (String) it.next();
                //noinspection unchecked
                T v = (T) subset.getProperty(k);
                map.put(k, v);
            }
        }
        return map;
    }

    public static File readFileNode(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    @NonNull String name) throws Exception {
        String value = config.getString(name);
        return readFileNode(value);
    }

    public static File readFileNode(@NonNull String path) throws Exception {
        if (!Strings.isNullOrEmpty(path)) {
            return PathUtils.readFile(path);
        }
        return null;
    }

    public static void checkStringValue(String value,
                                        @NonNull Class<?> caller,
                                        @NonNull String name) throws ConfigurationException {
        if (Strings.isNullOrEmpty(value)) {
            throw new ConfigurationException(
                    String.format("[%s] Missing configuration [name=%s]",
                            caller.getSimpleName(), name));
        }
    }
}
