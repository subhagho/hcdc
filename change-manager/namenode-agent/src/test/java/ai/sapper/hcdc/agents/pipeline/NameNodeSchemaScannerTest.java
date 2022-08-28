package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

class NameNodeSchemaScannerTest {
    private static final String CONFIG_FILE = "src/test/resources/configs/hdfs-files-scanner.xml";

    @Test
    void run() {
        try {
            HierarchicalConfiguration<ImmutableNode> config = ConfigReader.read(CONFIG_FILE, EConfigFileType.File);
            String name = NameNodeSchemaScanner.class.getSimpleName();
            NameNodeEnv.setup(name, getClass(), config);

            Preconditions.checkNotNull(NameNodeEnv.get(name).schemaManager());
            NameNodeSchemaScanner scanner = new NameNodeSchemaScanner(NameNodeEnv.get(name).stateManager(), name);
            scanner
                    .withSchemaManager(NameNodeEnv.get(name).schemaManager())
                    .init(NameNodeEnv.get(name).configNode(), NameNodeEnv.get(name).connectionManager());
            scanner.run();

            NameNodeEnv.dispose(name);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}