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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class HdfsHAConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-hdfs-ha";

    private static final String __UUID = UUID.randomUUID().toString();
    private static final String __PATH = String.format("/test/hcdc/core/HA/%s", __UUID);
    private static XMLConfiguration xmlConfiguration = null;
    private static ConnectionManager manager = new ConnectionManager();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = readFile();
        Preconditions.checkState(xmlConfiguration != null);
        manager.init(xmlConfiguration, null);
    }

    @Test
    void connect() {
        DefaultLogger.__LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            HdfsHAConnection connection = manager.getConnection(__CONNECTION_NAME, HdfsHAConnection.class);
            connection.connect();

            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            Path path = new Path(__PATH);
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
            HdfsHAConnection connection = manager.getConnection(__CONNECTION_NAME, HdfsHAConnection.class);
            connection.connect();

            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            Path path = new Path(__PATH);
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            path = new Path(String.format("%s/upload.xml", __PATH));
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