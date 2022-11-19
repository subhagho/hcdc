package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.utils.DemoEnv;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.configuration2.XMLConfiguration;
import org.glassfish.jersey.client.JerseyWebTarget;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

class WebServiceConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-ws";

    private static XMLConfiguration xmlConfiguration = null;

    private static ConnectionManager manager;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        DemoEnv env = new DemoEnv();
        env.init(xmlConfiguration);
        manager = env.connectionManager();
    }

    @Test
    void connect() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            WebServiceConnection ws = manager.getConnection(__CONNECTION_NAME, WebServiceConnection.class);

            JerseyWebTarget wt = ws.connect("jersey");
            Invocation.Builder builder = wt.request(MediaType.APPLICATION_JSON);
            Response response = builder.get();
            String data = response.readEntity(String.class);
            assertFalse(Strings.isNullOrEmpty(data));
            DefaultLogger.LOGGER.debug(String.format("DATA [%s]", data));

            manager.save(ws);
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}