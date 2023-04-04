package ai.sapper.cdc.common;

import ai.sapper.cdc.common.utils.DefaultLogger;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigReaderTest {
    @Getter
    @Setter
    public static class Database extends ConfigReader {
        @Config(name = "header.name")
        private String name;
        @Config(name = "header.version", type = Double.class)
        private double version;
        @Config(name = "header.type")
        private String type;
        @Config(name = "state.available", type = Boolean.class)
        private boolean available;
        @Config(name = "state.uptime", type = Long.class)
        private long uptime;
        @Config(name = ConfigReader.CONFIG_PARAMS)
        private Map<String, String> config;

        public Database(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                        @NonNull String path) {
            super(config, path);
        }
    }

    private static final String TEST_CONFIG_XML = "src/test/resources/configreader-test.xml";

    @Test
    void get() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "get"));
        try {
            ConfigReader reader = new ConfigReader(readFile(), "database");
            HierarchicalConfiguration<ImmutableNode> node = reader.get();
            assertNotNull(node);
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void readConfig() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "get"));
        try {
            Database reader = new Database(readFile(), "database");
            reader.read(Database.class);
            assertFalse(Strings.isNullOrEmpty(reader.name));
            assertEquals(10.2, reader.version);
            assertTrue(reader.available);
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void testGet() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s(name)", getClass().getCanonicalName(), "get"));
        try {
            ConfigReader reader = new ConfigReader(readFile(), "database");
            HierarchicalConfiguration<ImmutableNode> node = reader.get("header.name");
            assertNotNull(node);
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void getCollection() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s(name)", getClass().getCanonicalName(), "getCollection"));
        try {
            ConfigReader reader = new ConfigReader(readFile(), "database");
            List<HierarchicalConfiguration<ImmutableNode>> nodes = reader.getCollection("tables.table");
            assertNotNull(nodes);
            assertEquals(2, nodes.size());
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void readParameters() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s(name)", getClass().getCanonicalName(), "getCollection"));
        try {
            ConfigReader reader = new ConfigReader(readFile(), "database");
            Map<String, String> params = reader.readParameters();
            assertNotNull(params);
            assertEquals(2, params.size());
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void read() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "read"));
        try {
            ConfigReader reader = new ConfigReader(readFile(), "database");
            HierarchicalConfiguration<ImmutableNode> node = reader.get();
            assertNotNull(node);
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
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

    @Test
    void map() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "map"));
        try {
            ConfigReader reader = new ConfigReader(readFile(), "database");
            HierarchicalConfiguration<ImmutableNode> node = reader.get();
            assertNotNull(node);
            boolean ret = ConfigReader.checkIfNodeExists(node, "domains.abs");
            assertFalse(ret);
            ret = ConfigReader.checkIfNodeExists(node, "domains.mapping");
            assertTrue(ret);
            Map<String, String> map = ConfigReader.readAsMap(node, "domains.mapping");
            assertTrue(map.size() > 0);
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}