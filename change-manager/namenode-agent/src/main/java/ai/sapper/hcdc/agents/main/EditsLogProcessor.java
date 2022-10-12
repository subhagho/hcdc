package ai.sapper.hcdc.agents.main;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.Service;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.namenode.EditsLogReader;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

@Getter
public class EditsLogProcessor implements Service<NameNodeEnv.ENameNodeEnvState> {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    private EConfigFileType fileSource = EConfigFileType.File;
    private HierarchicalConfiguration<ImmutableNode> config;
    private EditsLogReader processor;
    private Thread runner;
    private NameNodeEnv env;
    @Override
    public Service<NameNodeEnv.ENameNodeEnvState> setConfigFile(@NonNull String path) {
        configFile = path;
        return this;
    }

    @Override
    public Service<NameNodeEnv.ENameNodeEnvState> setConfigSource(@NonNull String type) {
        configSource = type;
        return this;
    }

    public Service<NameNodeEnv.ENameNodeEnvState> init() throws Exception {
        try {
            Preconditions.checkState(!Strings.isNullOrEmpty(configFile));
            if (!Strings.isNullOrEmpty(configSource)) {
                fileSource = EConfigFileType.parse(configSource);
            }
            Preconditions.checkNotNull(fileSource);
            config = ConfigReader.read(configFile, fileSource);
            env = NameNodeEnv.setup(name(), getClass(), config);

            processor = new EditsLogReader(NameNodeEnv.get(getClass().getSimpleName()).stateManager(), name());
            processor.init(NameNodeEnv.get(name()).configNode(),
                    NameNodeEnv.get(name())
                            .connectionManager());
            return this;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.LOGGER.error(t.getLocalizedMessage());
            NameNodeEnv.get(name()).error(t);
            throw t;
        }
    }

    public Service<NameNodeEnv.ENameNodeEnvState> start() throws Exception {
        try {
            runner = new Thread(processor);
            runner.start();

            return this;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(env.LOG, t);
            DefaultLogger.error(env.LOG, t.getLocalizedMessage());
            NameNodeEnv.get(name()).error(t);
            throw t;
        }
    }

    @Override
    public Service<NameNodeEnv.ENameNodeEnvState> stop() throws Exception {
        NameNodeEnv.dispose(name());
        runner.join();
        return this;
    }

    @Override
    public NameNodeEnv.NameNodeEnvState status() {
        try {
            return NameNodeEnv.status(name());
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public String name() {
        return getClass().getSimpleName();
    }

    public long runOnce(@NonNull String configfile) throws Exception {
        this.configFile = configfile;
        init();
        try (DistributedLock lock = NameNodeEnv.get(name()).globalLock()) {
            lock.lock();
            try {
                return processor.doRun();
            } finally {
                lock.unlock();
            }
        }
    }

    public static void main(String[] args) {
        try {
            EditsLogProcessor runner = new EditsLogProcessor();
            JCommander.newBuilder().addObject(runner).build().parse(args);
            runner.init();
            runner.start();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.LOGGER.error(t.getLocalizedMessage());
            t.printStackTrace();
        }
    }
}
