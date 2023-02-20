package ai.sapper.cdc.core;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.TestUtils;
import ai.sapper.cdc.core.keystore.JavaKeyStore;
import ai.sapper.cdc.core.keystore.KeyStore;
import ai.sapper.cdc.core.utils.JavaKeyStoreUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

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
            String keyName = "oracle-demo-password";
            String keyValue = "test1234";
            String password = "test1234";

            JavaKeyStoreUtil util = new JavaKeyStoreUtil();
            util.setConfigFile(__CONFIG_FILE);
            util.setPassword(password);
            util.setKey(keyName);
            util.setValue(keyValue);
            util.run();

            util.setKey(UUID.randomUUID().toString());
            util.setValue("Dummy");
            util.run();

            HierarchicalConfiguration<ImmutableNode> configNode = xmlConfiguration.configurationAt("");
            KeyStore store = new JavaKeyStore().withPassword(password);
            store.init(configNode);

            //store.save(keyName, keyValue);
            //store.flush();
            String v = store.read(keyName);
            assertEquals(keyValue, v);

            //store.delete();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }
}