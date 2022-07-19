package ai.sapper.hcdc.agents.namenode.main;

import ai.sapper.hcdc.agents.namenode.EditLogProcessor;
import ai.sapper.hcdc.agents.namenode.NameNodeEnv;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

@Getter
@Setter
public class EditLogRunner {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configfile;
    private HierarchicalConfiguration<ImmutableNode> config;
    private EditLogProcessor processor;
    private Thread runner;

    private void init() throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(configfile));

        config = ConfigReader.read(configfile);
        NameNodeEnv.setup(config);

        processor = new EditLogProcessor(NameNodeEnv.stateManager());
        processor.init(NameNodeEnv.get().configNode(), NameNodeEnv.connectionManager());
    }

    private void run() throws Exception {
        runner = new Thread(processor);
        runner.start();
    }

    public long runOnce(@NonNull String configfile) throws Exception {
        try {
            this.configfile = configfile;
            init();
            return processor.doRun();
        } finally {
            NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
            DefaultLogger.LOG.warn(String.format("Edit Log Processor Shutdown...[state=%s]", state.name()));
        }
    }

    public static void main(String[] args) {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
                    DefaultLogger.LOG.warn(String.format("Edit Log Processor Shutdown...[state=%s]", state.name()));
                }
            });
            EditLogRunner runner = new EditLogRunner();
            JCommander.newBuilder().addObject(runner).build().parse(args);
            runner.init();
            runner.run();
            runner.runner.join();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
