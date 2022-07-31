package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.services.EConfigFileType;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.HdfsHAConnection;
import ai.sapper.hcdc.core.model.DFSFileState;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.jupiter.api.Assertions.*;

class ParquetConverterTest {
    private static final String CONFIG_FILE = "src/test/resources/parquet-test.xml";
    private static final String FILE = "src/test/resources/data/links_1.parquet";
    private static final String OUTDIR = "C:\\Work\\temp\\output";
    private static final String HDFS_PARQUET_FILE = "/test/hcdc/loader/parquet/data/train.parquet";
    private static final String CONNECTION_NAME = "source-hdfs-ha";

    @Test
    void convert() {
        try {
            ParquetConverter converter = new ParquetConverter();
            File inf = new File(FILE);
            try (FileInputStream fos = new FileInputStream(inf)) {
                byte[] array = new byte[32];
                int r = fos.read(array);
                boolean valid = converter.detect(inf.getAbsolutePath(), array, r);
                assertTrue(valid);
            }
            String name = FilenameUtils.getName(inf.getAbsolutePath());
            name = FilenameUtils.removeExtension(name);
            File outf = new File(String.format("%s/%s.avro", OUTDIR, name));
            converter.convert(inf, outf);

            DefaultLogger.LOG.info(String.format("Generated output: [path=%s]", outf.getAbsolutePath()));
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void extractSchema() {
        try {
            HierarchicalConfiguration<ImmutableNode> config = ConfigReader.read(CONFIG_FILE, EConfigFileType.File);
            NameNodeEnv.setup(config);
            ZkStateManager stateManager = NameNodeEnv.stateManager();
            assertNotNull(stateManager);
            DFSFileState fileState = stateManager.fileStateHelper().get(HDFS_PARQUET_FILE);
            assertNotNull(fileState);

            HdfsHAConnection connection = NameNodeEnv.connectionManager()
                    .getConnection(CONNECTION_NAME, HdfsHAConnection.class);
            connection.connect();

            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            try (HDFSBlockReader reader = new HDFSBlockReader(connection.dfsClient(), HDFS_PARQUET_FILE)) {
                reader.init();
                ParquetConverter converter = new ParquetConverter();
                File outf = converter.extractSchema(reader, new File(OUTDIR), fileState);
                DefaultLogger.LOG.info(String.format("Generated schema file: [path=%s]", outf.getAbsolutePath()));
            }
        } catch (Exception t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}