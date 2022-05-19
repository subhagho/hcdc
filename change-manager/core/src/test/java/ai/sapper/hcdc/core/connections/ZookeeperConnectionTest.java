package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.DefaultLogger;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.curator.utils.ZKPaths;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ZookeeperConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-zk";
    private static final String __UUID = UUID.randomUUID().toString();
    private static final String __PATH = String.format("/test/hcdc/core/zookeeper/%s", __UUID);

    private static XMLConfiguration xmlConfiguration = null;

    private static ConnectionManager manager = new ConnectionManager();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        manager.init(xmlConfiguration, null);
    }


    @Test
    void connect() {
        DefaultLogger.__LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            ZookeeperConnection connection = manager.getConnection(__CONNECTION_NAME, ZookeeperConnection.class);
            assertNotNull(connection);
            connection.connect();
            assertEquals(Connection.EConnectionState.Connected, connection.connectionState());
            ZKPaths.mkdirs(connection.client().getZookeeperClient().getZooKeeper(), __PATH);

            connection.close();
            assertEquals(Connection.EConnectionState.Closed, connection.connectionState());
        } catch (Throwable t) {
            DefaultLogger.__LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}