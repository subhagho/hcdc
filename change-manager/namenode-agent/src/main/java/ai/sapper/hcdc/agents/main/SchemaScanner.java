package ai.sapper.hcdc.agents.main;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.pipeline.NameNodeSchemaScanner;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

@Getter
@Setter
public class SchemaScanner {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configfile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    @Setter(AccessLevel.NONE)
    private EConfigFileType fileSource = EConfigFileType.File;
    @Setter(AccessLevel.NONE)
    private HierarchicalConfiguration<ImmutableNode> config;
    @Setter(AccessLevel.NONE)
    private NameNodeSchemaScanner scanner;

    public void init() throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(configfile));
        if (!Strings.isNullOrEmpty(configSource)) {
            fileSource = EConfigFileType.parse(configSource);
        }
        Preconditions.checkNotNull(fileSource);
        config = ConfigReader.read(configfile, fileSource);
        NameNodeEnv.setup(config);
        NameNodeSchemaScanner scanner = new NameNodeSchemaScanner(NameNodeEnv.stateManager());
        scanner
                .withSchemaManager(NameNodeEnv.get().schemaManager())
                .init(NameNodeEnv.get().configNode(), NameNodeEnv.connectionManager());
    }

    public void run() throws Exception {
        Preconditions.checkNotNull(scanner);
        scanner.run();
    }

    public static void main(String[] args) {
        try {
            SchemaScanner runner = new SchemaScanner();
            JCommander.newBuilder().addObject(runner).build().parse(args);
            runner.init();
            runner.run();
        } catch (Exception ex) {
            DefaultLogger.LOG.error(ex.getLocalizedMessage());
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(ex));
        } finally {
            NameNodeEnv.dispose();
        }
    }
}
