package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.ZookeeperSettings;
import ai.sapper.cdc.core.utils.DemoEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.curator.utils.ZKPaths;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ZookeeperConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-zk";
    private static final String __UUID = UUID.randomUUID().toString();
    private static final String __BASE_PATH = "/test/hcdc/core/zookeeper";
    private static final String __PATH = String.format("%s/%s", __BASE_PATH, __UUID);

    private static XMLConfiguration xmlConfiguration = null;

    private static ConnectionManager manager;
    private static DemoEnv env = new DemoEnv();
    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.init(xmlConfiguration);
        manager = env.connectionManager();
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }


    @Test
    void connect() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            ZookeeperConnection connection = manager.getConnection(__CONNECTION_NAME, ZookeeperConnection.class);
            assertNotNull(connection);
            connection.connect();

            manager.save(connection);
            assertEquals(Connection.EConnectionState.Connected, connection.connectionState());
            ZKPaths.mkdirs(connection.client().getZookeeperClient().getZooKeeper(), __PATH);

            List<String> paths = connection.client().getChildren().forPath(__BASE_PATH);
            assertNotNull(paths);
            DefaultLogger.LOGGER.debug(String.format("PATHS : [%s]", paths));

            connection.close();
            assertEquals(Connection.EConnectionState.Closed, connection.connectionState());

            ZookeeperSettings settings = connection.settings();
            Map<String, String> values = ConnectionSettings.serialize(settings);
            assertNotNull(values);
            assertFalse(values.isEmpty());

            settings = (ZookeeperSettings) ConnectionSettings.read(values);
            assertNotNull(settings);
            settings.validate();
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}