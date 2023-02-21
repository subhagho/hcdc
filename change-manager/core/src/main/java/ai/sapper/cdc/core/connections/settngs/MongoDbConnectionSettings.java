package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.db.DbConnectionConfig;
import ai.sapper.cdc.core.connections.db.MongoDbConnection;
import ai.sapper.cdc.core.model.Encrypted;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MongoDbConnectionSettings extends ConnectionSettings {
    public static class Constants {
        public static final String CONFIG_HOST = "host";
        public static final String CONFIG_PORT = "port";
        public static final String CONFIG_DB = "db";
    }

    @Setting(name = DbConnectionConfig.Constants.CONFIG_USER)
    private String user;
    @Setting(name = Constants.CONFIG_HOST)
    private String host;
    @Setting(name = Constants.CONFIG_PORT, required = false)
    private int port = 27017;
    @Setting(name = Constants.CONFIG_DB)
    private String db;
    @Encrypted
    @Setting(name = DbConnectionConfig.Constants.CONFIG_PASS_KEY)
    private String password;
    @Setting(name = DbConnectionConfig.Constants.CONFIG_POOL_SIZE, required = false)
    private int poolSize = 32;

    public MongoDbConnectionSettings() {
        setConnectionClass(MongoDbConnection.class);
        setType(EConnectionType.db);
    }

    @Override
    public void validate() throws Exception {
        ConfigReader.checkStringValue(getName(), getClass(), ConnectionConfig.CONFIG_NAME);
        ConfigReader.checkStringValue(getUser(), getClass(), DbConnectionConfig.Constants.CONFIG_USER);
        ConfigReader.checkStringValue(getPassword(), getClass(), DbConnectionConfig.Constants.CONFIG_PASS_KEY);
        ConfigReader.checkStringValue(getHost(), getClass(), Constants.CONFIG_HOST);
        ConfigReader.checkStringValue(getDb(), getClass(), Constants.CONFIG_DB);
    }
}
