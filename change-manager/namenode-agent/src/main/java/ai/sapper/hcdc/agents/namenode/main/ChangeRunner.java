package ai.sapper.hcdc.agents.namenode.main;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.namenode.SourceChangeDeltaProcessor;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.services.EConfigFileType;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

@Getter
@Setter
public class ChangeRunner {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configfile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    private EConfigFileType fileSource = EConfigFileType.File;
    private HierarchicalConfiguration<ImmutableNode> config;
    private Thread runner;
    private SourceChangeDeltaProcessor processor;

    private void init() throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(configfile));
        if (!Strings.isNullOrEmpty(configSource)) {
            fileSource = EConfigFileType.parse(configSource);
        }
        Preconditions.checkNotNull(fileSource);
        config = ConfigReader.read(configfile, fileSource);
        NameNodeEnv.setup(config);

        processor = new SourceChangeDeltaProcessor(NameNodeEnv.stateManager());
        processor.init(NameNodeEnv.get().configNode(), NameNodeEnv.connectionManager());
    }

    private void run() throws Exception {
        runner = new Thread(processor);
        runner.start();
    }

    public static void main(String[] args) {
        try {
            ChangeRunner runner = new ChangeRunner();
            JCommander.newBuilder().addObject(runner).build().parse(args);
            runner.init();
            runner.run();
            runner.runner.join();
        } catch (Throwable t) {
            t.printStackTrace();
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            DefaultLogger.LOG.error(t.getLocalizedMessage());
        }
    }
}
