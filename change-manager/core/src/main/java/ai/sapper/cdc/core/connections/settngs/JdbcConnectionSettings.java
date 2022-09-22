package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.db.DbConnectionConfig;
import ai.sapper.cdc.core.connections.db.JdbcConnection;
import ai.sapper.cdc.core.model.Encrypted;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JdbcConnectionSettings extends ConnectionSettings {
    @Setting(name = JdbcConnection.JdbcConnectionConfig.Constants.CONFIG_DRIVER)
    private String jdbcDriver;
    @Setting(name = JdbcConnection.JdbcConnectionConfig.Constants.CONFIG_DIALECT, required = false)
    private String jdbcDialect;
    @Setting(name = DbConnectionConfig.Constants.CONFIG_JDBC_URL)
    private String jdbcUrl;
    @Setting(name = DbConnectionConfig.Constants.CONFIG_DB_NAME, required = false)
    private String db;
    @Setting(name = DbConnectionConfig.Constants.CONFIG_USER)
    private String user;
    @Encrypted
    @Setting(name = DbConnectionConfig.Constants.CONFIG_PASS_KEY)
    private String password;
    @Setting(name = DbConnectionConfig.Constants.CONFIG_POOL_SIZE, required = false)
    private int poolSize = 32;

    public JdbcConnectionSettings() {
        setConnectionClass(JdbcConnection.class);
        setType(EConnectionType.db);
    }

    @Override
    public void validate() throws Exception {
        ConfigReader.checkStringValue(getName(), getClass(), ConnectionConfig.CONFIG_NAME);
        ConfigReader.checkStringValue(getJdbcUrl(), getClass(), DbConnectionConfig.Constants.CONFIG_JDBC_URL);
        ConfigReader.checkStringValue(getUser(), getClass(), DbConnectionConfig.Constants.CONFIG_USER);
        ConfigReader.checkStringValue(getPassword(), getClass(), DbConnectionConfig.Constants.CONFIG_PASS_KEY);
    }
}
