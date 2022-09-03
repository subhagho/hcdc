package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.core.model.Encrypted;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class S2ConnectionSettings extends ConnectionSettings {
    public static final String JDBC_S2_PREFIX = "jdbc:singlestore";

    private String jdbcUrl;
    private String db;
    private String user;
    @Encrypted
    private String password;
    private int poolSize = 32;
}
