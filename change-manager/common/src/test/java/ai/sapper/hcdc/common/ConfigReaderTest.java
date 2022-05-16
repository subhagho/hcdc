package ai.sapper.hcdc.common;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class ConfigReaderTest {
    private static final String TEST_CONFIG_XML = "src/test/resources/configreader-test.xml";

    @Test
    void get() {
    }

    @Test
    void testGet() {
    }

    @Test
    void getCollection() {
    }

    @Test
    void readParameters() {
    }

    @Test
    void read() {
        try {
            readFile();
        } catch (Throwable t) {
            DefaultLogger.__LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    private XMLConfiguration readFile() throws Exception {
        File cf = new File(TEST_CONFIG_XML);
        if (!cf.exists()) {
            throw new Exception(String.format("Configuration file not found. ]path=%s]", cf.getAbsolutePath()));
        }
        Configurations configs = new Configurations();
        return configs.xml(cf);
    }
}