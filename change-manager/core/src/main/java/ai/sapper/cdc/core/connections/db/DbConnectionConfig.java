package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.settngs.JdbcConnectionSettings;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public class DbConnectionConfig extends ConfigReader {
    public static class Constants {
        public static final String CONFIG_NAME = "name";
        public static final String CONFIG_JDBC_URL = "jdbcUrl";
        public static final String CONFIG_USER = "user";
        public static final String CONFIG_PASS_KEY = "passwordKay";
        public static final String CONFIG_POOL_SIZE = "poolSize";
        public static final String CONFIG_DB_NAME = "db";
    }

    private final JdbcConnectionSettings settings = new JdbcConnectionSettings();

    public DbConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull String path) {
        super(config, path);
    }

    public JdbcConnectionSettings read() throws ConfigurationException {
        if (get() == null) {
            throw new ConfigurationException("JDBC Configuration not drt or is NULL");
        }
        try {
            settings.setName(get().getString(Constants.CONFIG_NAME));
            if (Strings.isNullOrEmpty(settings.getName())) {
                throw new ConfigurationException(
                        String.format("JDBC Configuration Error: missing [%s]", Constants.CONFIG_NAME));
            }
            settings.setJdbcUrl(get().getString(Constants.CONFIG_JDBC_URL));
            if (Strings.isNullOrEmpty(settings.getJdbcUrl())) {
                throw new ConfigurationException(
                        String.format("JDBC Configuration Error: missing [%s]", Constants.CONFIG_JDBC_URL));
            }
            settings.setUser(get().getString(Constants.CONFIG_USER));
            if (Strings.isNullOrEmpty(settings.getUser())) {
                throw new ConfigurationException(
                        String.format("JDBC Configuration Error: missing [%s]", Constants.CONFIG_USER));
            }
            settings.setPassword(get().getString(Constants.CONFIG_PASS_KEY));
            if (Strings.isNullOrEmpty(settings.getPassword())) {
                throw new ConfigurationException(
                        String.format("JDBC Configuration Error: missing [%s]", Constants.CONFIG_PASS_KEY));
            }
            String s = get().getString(Constants.CONFIG_POOL_SIZE);
            if (!Strings.isNullOrEmpty(s)) {
                settings.setPoolSize(Integer.parseInt(s));
            }
            s = get().getString(Constants.CONFIG_DB_NAME);
            if (!Strings.isNullOrEmpty(s)) {
                settings.setDb(s);
            }
            settings.setParameters(readParameters());

            return settings;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
