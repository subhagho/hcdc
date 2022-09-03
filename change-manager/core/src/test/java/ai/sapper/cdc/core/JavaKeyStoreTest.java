package ai.sapper.cdc.core;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.TestUtils;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class JavaKeyStoreTest {
    private static final String __CONFIG_FILE = "src/test/resources/keystore.xml";
    private static final String __CONFIG_PATH = "config";
    private static XMLConfiguration xmlConfiguration = null;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
    }

    @Test
    void read() {
        try {
            String keyName = UUID.randomUUID().toString();
            String keyValue = "This is a test key";

            HierarchicalConfiguration<ImmutableNode> configNode = xmlConfiguration.configurationAt(__CONFIG_PATH);
            KeyStore store = new JavaKeyStore().withPassword("password");
            store.init(configNode);

            store.save(keyName, keyValue);
            String v = store.read(keyName);
            assertEquals(keyValue, v);

            store.delete();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }
}