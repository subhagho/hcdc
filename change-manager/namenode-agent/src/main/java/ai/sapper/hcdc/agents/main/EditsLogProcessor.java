package ai.sapper.hcdc.agents.main;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.namenode.EditsLogReader;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

@Getter
public class EditsLogProcessor implements Service<NameNodeEnv.ENameNEnvState> {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    private EConfigFileType fileSource = EConfigFileType.File;
    @Setter(AccessLevel.NONE)
    private HierarchicalConfiguration<ImmutableNode> config;
    @Setter(AccessLevel.NONE)
    private EditsLogReader processor;
    @Setter(AccessLevel.NONE)
    private Thread runner;

    @Override
    public Service<NameNodeEnv.ENameNEnvState> setConfigFile(@NonNull String path) {
        configFile = path;
        return this;
    }

    @Override
    public Service<NameNodeEnv.ENameNEnvState> setConfigSource(@NonNull String type) {
        configSource = type;
        return this;
    }

    public Service<NameNodeEnv.ENameNEnvState> init() throws Exception {
        try {
            Preconditions.checkState(!Strings.isNullOrEmpty(configFile));
            if (!Strings.isNullOrEmpty(configSource)) {
                fileSource = EConfigFileType.parse(configSource);
            }
            Preconditions.checkNotNull(fileSource);
            config = ConfigReader.read(configFile, fileSource);
            NameNodeEnv.setup(name(), config);

            processor = new EditsLogReader(NameNodeEnv.get(getClass().getSimpleName()).stateManager(), name());
            processor.init(NameNodeEnv.get(name()).configNode(),
                    NameNodeEnv.get(name())
                            .connectionManager());
            return this;
        } catch (Throwable t) {
            NameNodeEnv.get(name()).error(t);
            throw t;
        }
    }

    public Service<NameNodeEnv.ENameNEnvState> start() throws Exception {
        try {
            runner = new Thread(processor);
            runner.start();

            return this;
        } catch (Throwable t) {
            NameNodeEnv.get(name()).error(t);
            throw t;
        }
    }

    @Override
    public Service<NameNodeEnv.ENameNEnvState> stop() throws Exception {
        NameNodeEnv.dispose(name());
        runner.join();
        return this;
    }

    @Override
    public NameNodeEnv.NameNEnvState status() {
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
            t.printStackTrace();
        }
    }
}
