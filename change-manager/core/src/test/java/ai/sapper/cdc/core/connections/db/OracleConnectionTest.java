package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.utils.DemoEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

class OracleConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/test-oracle-env.xml";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoEnv env = new DemoEnv();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.init(xmlConfiguration);
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }

    @Test
    void connect() {
        try {
            OracleConnection connection = env.connectionManager().getConnection("test-oracle", OracleConnection.class);
            assertNotNull(connection);
            connection.connect();
            assertTrue(connection.isConnected());
            try (Connection sqlc = connection.getConnection()) {
                Statement stmt = sqlc.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT sysdate FROM dual");
                rs.next();
                Timestamp ts = rs.getTimestamp(1);
                System.out.println(ts);
            }
            env.connectionManager().save();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}