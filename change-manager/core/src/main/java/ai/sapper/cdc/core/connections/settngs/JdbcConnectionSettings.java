package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.core.connections.db.JdbcConnection;
import ai.sapper.cdc.core.model.Encrypted;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JdbcConnectionSettings extends ConnectionSettings {
    private String jdbcDriver;
    private String jdbcDialect;
    private String jdbcUrl;
    private String db;
    private String user;
    @Encrypted
    private String password;
    private int poolSize = 32;

    public JdbcConnectionSettings() {
        setConnectionType(JdbcConnection.class);
    }
}
