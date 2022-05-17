package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.DefaultLogger;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
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

class HdfsConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-hdfs";
    private static final String __PATH = UUID.randomUUID().toString();

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
        DefaultLogger.__LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            HdfsConnection connection = manager.getConnection(__CONNECTION_NAME, HdfsConnection.class);
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
            HdfsConnection connection = manager.getConnection(__CONNECTION_NAME, HdfsConnection.class);
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
}