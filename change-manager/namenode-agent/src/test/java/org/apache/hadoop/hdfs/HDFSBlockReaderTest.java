package org.apache.hadoop.hdfs;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.hadoop.HdfsHAConnection;
import ai.sapper.cdc.core.model.HDFSBlockData;
import ai.sapper.cdc.core.utils.DemoEnv;
import ai.sapper.hcdc.agents.namenode.TestUtils;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class HDFSBlockReaderTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-hdfs-ha";

    private static final String __UUID = UUID.randomUUID().toString();
    private static final String __PATH = String.format("/test/hcdc/core/HA/%s", __UUID);
    private static XMLConfiguration xmlConfiguration = null;
    private static ConnectionManager manager = new ConnectionManager();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        DemoEnv env = new DemoEnv();
        env.init(xmlConfiguration);
        manager.init(xmlConfiguration, env, env.name());
    }

    @Test
    void read() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "close"));
        try {
            HdfsHAConnection connection = manager.getConnection(__CONNECTION_NAME, HdfsHAConnection.class);
            connection.connect();

            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            Path path = new Path(__PATH);
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            String f = String.format("%s/upload.xml", __PATH);
            path = new Path(f);
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
            HDFSBlockReader reader = new HDFSBlockReader(connection.dfsClient(), f);
            reader.init(null);

            Thread.sleep(5000);

            LocatedBlocks locatedBlocks = reader.locatedBlocks();
            assertNotNull(locatedBlocks);

            for (LocatedBlock blk : locatedBlocks.getLocatedBlocks()) {
                long size = blk.getBlock().getNumBytes() - 100;
                HDFSBlockData data = reader.read(blk.getBlock().getBlockId(), 0, 100, -1);
                assertNotNull(data);
                assertNotNull(data.data());
                while (data.data().hasRemaining()) {
                    data.data().get();
                    size--;
                }
                assertEquals(0, size);
            }
            connection.close();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}