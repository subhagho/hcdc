package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WebServiceConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-ws";

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
        DefaultLogger.LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            WebServiceConnection ws = manager.getConnection(__CONNECTION_NAME, WebServiceConnection.class);

            WebTarget wt = ws.connect("jersey");
            Invocation.Builder builder = wt.request(MediaType.APPLICATION_JSON);
            Response response = builder.get();
            String data = response.readEntity(String.class);
            assertFalse(Strings.isNullOrEmpty(data));
            DefaultLogger.LOG.debug(String.format("DATA [%s]", data));
        } catch (Throwable t) {
            DefaultLogger.LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}