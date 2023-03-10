package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.utils.DemoEnv;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableItem;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AzureTableConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/test-azure-table-env.xml";
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
            AzureTableConnection connection = env.connectionManager()
                    .getConnection("azure-table-test", AzureTableConnection.class);
            assertNotNull(connection);
            connection.connect();
            assertTrue(connection.isConnected());
            TableServiceClient client = connection.client();
            client.createTableIfNotExists("TEST_DEMO");
            int count = 0;
            for (TableItem table : client.listTables()) {
                count++;
                System.out.printf("TABLE=[%s]%n", table.getName());
            }
            assertTrue(count > 0);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}