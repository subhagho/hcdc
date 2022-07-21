package ai.sapper.hcdc.agents.namenode.main;

import ai.sapper.hcdc.agents.namenode.HDFSDeltaChangeProcessor;
import ai.sapper.hcdc.agents.namenode.HDFSSnapshotProcessor;
import ai.sapper.hcdc.agents.namenode.NameNodeEnv;
import ai.sapper.hcdc.common.ConfigReader;
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
public class SnapshotRunner {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configfile;
    private HierarchicalConfiguration<ImmutableNode> config;
    private HDFSSnapshotProcessor processor;

    private void init() throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(configfile));

        config = ConfigReader.read(configfile);
        NameNodeEnv.setup(config);

        processor = new HDFSSnapshotProcessor(NameNodeEnv.stateManager());
        processor.init(NameNodeEnv.get().configNode(), NameNodeEnv.connectionManager());
    }

    public static void main(String[] args) {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
                    DefaultLogger.LOG.warn(String.format("Edit Log Processor Shutdown...[state=%s]", state.name()));
                }
            });
            SnapshotRunner runner = new SnapshotRunner();
            JCommander.newBuilder().addObject(runner).build().parse(args);
            runner.init();
            runner.processor.run();
        } catch (Throwable t) {
            t.printStackTrace();
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            DefaultLogger.LOG.error(t.getLocalizedMessage());
        }
    }
}
