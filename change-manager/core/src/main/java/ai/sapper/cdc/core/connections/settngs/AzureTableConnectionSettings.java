package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.db.AzureTableConnection;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class AzureTableConnectionSettings extends ConnectionSettings {
    public static class Constants {
        public static final String CONFIG_CONNECTION_STRING = "connectionString";
        public static final String CONFIG_DB_NAME = "db";
    }

    @Setting(name = Constants.CONFIG_CONNECTION_STRING)
    private String connectionString;
    @Setting(name = Constants.CONFIG_DB_NAME)
    private String db;

    public AzureTableConnectionSettings() {
        setConnectionClass(AzureTableConnection.class);
        setType(EConnectionType.db);
    }

    public AzureTableConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof AzureTableConnectionSettings);
        connectionString = ((AzureTableConnectionSettings) settings).getConnectionString();
    }

    @Override
    public void validate() throws Exception {
        ConfigReader.checkStringValue(getName(), getClass(), ConnectionConfig.CONFIG_NAME);
        ConfigReader.checkStringValue(connectionString, getClass(), Constants.CONFIG_CONNECTION_STRING);
        ConfigReader.checkStringValue(db, getClass(), Constants.CONFIG_DB_NAME);
    }
}
