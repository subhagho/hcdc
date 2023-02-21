package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.settngs.MongoDbConnectionSettings;
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
public class MongoDbConnectionConfig extends ConnectionConfig {
    public static class Constants {
        public static final String __CONFIG_PATH = "mongodb";
    }

    private final MongoDbConnectionSettings settings = new MongoDbConnectionSettings();

    public MongoDbConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
        super(config, Constants.__CONFIG_PATH);
    }

    public MongoDbConnectionSettings read() throws ConfigurationException {
        settings.setName(get().getString(ConnectionConfig.CONFIG_NAME));
        checkStringValue(settings.getName(), getClass(), ConnectionConfig.CONFIG_NAME);
        settings.setUser(get().getString(DbConnectionConfig.Constants.CONFIG_USER));
        checkStringValue(settings.getUser(), getClass(), DbConnectionConfig.Constants.CONFIG_USER);
        settings.setPassword(get().getString(DbConnectionConfig.Constants.CONFIG_PASS_KEY));
        checkStringValue(settings.getPassword(), getClass(), DbConnectionConfig.Constants.CONFIG_PASS_KEY);
        settings.setHost(get().getString(MongoDbConnectionSettings.Constants.CONFIG_HOST));
        checkStringValue(settings.getHost(), getClass(), MongoDbConnectionSettings.Constants.CONFIG_HOST);
        String s = get().getString(MongoDbConnectionSettings.Constants.CONFIG_PORT);
        if (!Strings.isNullOrEmpty(s)) {
            settings.setPort(Integer.parseInt(s));
        }
        settings.setDb(get().getString(MongoDbConnectionSettings.Constants.CONFIG_DB));
        checkStringValue(settings.getDb(), getClass(), MongoDbConnectionSettings.Constants.CONFIG_DB);
        s = get().getString(DbConnectionConfig.Constants.CONFIG_POOL_SIZE);
        if (!Strings.isNullOrEmpty(s)) {
            settings.setPoolSize(Integer.parseInt(s));
        }
        settings.setParameters(readParameters());
        return settings;
    }
}
