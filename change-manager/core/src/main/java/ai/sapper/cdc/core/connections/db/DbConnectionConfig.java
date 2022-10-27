package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.core.connections.ConnectionConfig;
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
public class DbConnectionConfig extends ConnectionConfig {
    public static class Constants {
        public static final String CONFIG_JDBC_URL = "jdbcUrl";
        public static final String CONFIG_USER = "user";
        public static final String CONFIG_PASS_KEY = "passwordKey";
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
            settings.setName(get().getString(ConnectionConfig.CONFIG_NAME));
            checkStringValue(settings.getName(), getClass(), ConnectionConfig.CONFIG_NAME);
            settings.setJdbcUrl(get().getString(Constants.CONFIG_JDBC_URL));
            checkStringValue(settings.getJdbcUrl(), getClass(), Constants.CONFIG_JDBC_URL);
            settings.setUser(get().getString(Constants.CONFIG_USER));
            checkStringValue(settings.getUser(), getClass(), Constants.CONFIG_USER);
            settings.setPassword(get().getString(Constants.CONFIG_PASS_KEY));
            checkStringValue(settings.getPassword(), getClass(), Constants.CONFIG_PASS_KEY);
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
