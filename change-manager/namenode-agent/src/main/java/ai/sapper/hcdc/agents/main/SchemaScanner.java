package ai.sapper.hcdc.agents.main;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.Service;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.NameNodeError;
import ai.sapper.hcdc.agents.pipeline.NameNodeSchemaScanner;
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
public class SchemaScanner implements Service<NameNodeEnv.ENameNodeEnvState> {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    @Setter(AccessLevel.NONE)
    private EConfigFileType fileSource = EConfigFileType.File;
    @Setter(AccessLevel.NONE)
    private HierarchicalConfiguration<ImmutableNode> config;
    @Setter(AccessLevel.NONE)
    private NameNodeSchemaScanner scanner;
    @Setter(AccessLevel.NONE)
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
            scanner = new NameNodeSchemaScanner(NameNodeEnv.get(name()).stateManager(), name());
            scanner
                    .withSchemaManager(NameNodeEnv.get(name()).schemaManager())
                    .init(NameNodeEnv.get(name()).configNode(), NameNodeEnv.get(name()).connectionManager());

            return this;
        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(t.getLocalizedMessage());
            DefaultLogger.stacktrace(t);
            throw new NameNodeError(t);
        }
    }

    @Override
    public Service<NameNodeEnv.ENameNodeEnvState> start() throws Exception {
        try {
            Preconditions.checkNotNull(scanner);
            scanner.run();
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

    public static void main(String[] args) {
        try {
            SchemaScanner runner = new SchemaScanner();
            JCommander.newBuilder().addObject(runner).build().parse(args);
            runner.init();
            runner.start();
            runner.stop();
        } catch (Exception ex) {
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(ex));
        }
    }
}
