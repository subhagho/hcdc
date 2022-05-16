package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.DefaultLogger;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class HdfsConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __PATH_PREFIX = "connections";

    private static XMLConfiguration xmlConfiguration = null;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = readFile();
    }

    @Test
    void init() {
        DefaultLogger.__LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "init"));
        try {
            HdfsConnection connection = new HdfsConnection();
            connection.init(xmlConfiguration, __PATH_PREFIX);
        } catch (Throwable t) {
            DefaultLogger.__LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void connect() {
        DefaultLogger.__LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            HdfsConnection connection = new HdfsConnection();
            connection.init(xmlConfiguration, __PATH_PREFIX);
            connection.connect();
        } catch (Throwable t) {
            DefaultLogger.__LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void close() {
        DefaultLogger.__LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            HdfsConnection connection = new HdfsConnection();
            connection.init(xmlConfiguration, __PATH_PREFIX);
            connection.connect();
            connection.close();
        } catch (Throwable t) {
            DefaultLogger.__LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    private static XMLConfiguration readFile() throws Exception {
        File cf = new File(__CONFIG_FILE);
        if (!cf.exists()) {
            throw new Exception(String.format("Configuration file not found. ]path=%s]", cf.getAbsolutePath()));
        }
        Configurations configs = new Configurations();
        return configs.xml(cf);
    }
}