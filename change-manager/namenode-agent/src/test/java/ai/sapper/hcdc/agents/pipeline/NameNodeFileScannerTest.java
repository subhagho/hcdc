package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.services.EConfigFileType;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NameNodeFileScannerTest {
    private static final String CONFIG_FILE = "src/test/resources/configs/hdfs-files-scanner.xml";

    @Test
    void run() {
        try {
            HierarchicalConfiguration<ImmutableNode> config = ConfigReader.read(CONFIG_FILE, EConfigFileType.File);
            NameNodeEnv.setup(config);

            Preconditions.checkNotNull(NameNodeEnv.get().schemaManager());
            NameNodeFileScanner scanner = new NameNodeFileScanner(NameNodeEnv.stateManager());
            scanner
                    .withSchemaManager(NameNodeEnv.get().schemaManager())
                    .init(NameNodeEnv.get().configNode(), NameNodeEnv.connectionManager());
            scanner.run();

            NameNodeEnv.dispose();
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}