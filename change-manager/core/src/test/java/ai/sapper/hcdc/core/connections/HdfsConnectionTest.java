package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.DefaultLogger;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class HdfsConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __PATH_PREFIX = "connections";
    private static final String __PATH = UUID.randomUUID().toString();

    private static XMLConfiguration xmlConfiguration = null;
    private static HierarchicalConfiguration<ImmutableNode> config = null;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = readFile();
        Preconditions.checkState(xmlConfiguration != null);
        config = xmlConfiguration.configurationAt(__PATH_PREFIX);
        Preconditions.checkState(config != null);
    }

    @Test
    void init() {
        DefaultLogger.__LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "init"));
        try {
            HdfsConnection connection = new HdfsConnection();
            connection.init(config);
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
            connection.init(config);
            connection.connect();
            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            Path path = new Path(String.format("/test/hcdc/core/%s", __PATH));
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }

        } catch (Throwable t) {
            DefaultLogger.__LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void close() {
        DefaultLogger.__LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "close"));
        try {
            HdfsConnection connection = new HdfsConnection();
            connection.init(config);
            connection.connect();
            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            Path path = new Path(String.format("/test/hcdc/core/%s", __PATH));
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            path = new Path(String.format("/test/hcdc/core/%s/upload.xml", __PATH));
            FSDataOutputStream fsDataOutputStream = fs.create(path, true);
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))) {
                File file = new File(__CONFIG_FILE);    //creates a new file instance
                FileReader fr = new FileReader(file);   //reads the file
                try (BufferedReader reader = new BufferedReader(fr)) {  //creates a buffering character input stream
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write(line);
                        writer.newLine();
                    }
                }
            }

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