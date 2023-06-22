/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.hcdc.agents.main;

import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.NameNodeError;
import ai.sapper.cdc.core.Service;
import ai.sapper.cdc.core.processing.ProcessorState;
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
public class SchemaScanner implements Service<ProcessorState.EProcessorState> {
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
    private final ProcessorState state = new ProcessorState();

    @Override
    public SchemaScanner setConfigFile(@NonNull String path) {
        configFile = path;
        return this;
    }

    @Override
    public SchemaScanner setConfigSource(@NonNull String type) {
        configSource = type;
        return this;
    }

    public SchemaScanner init() throws Exception {
        try {
            Preconditions.checkState(!Strings.isNullOrEmpty(configFile));
            if (!Strings.isNullOrEmpty(configSource)) {
                fileSource = EConfigFileType.parse(configSource);
            }
            Preconditions.checkNotNull(fileSource);
            config = ConfigReader.read(configFile, fileSource);
            env = NameNodeEnv.setup(name(), getClass(), config);
            scanner = new NameNodeSchemaScanner(env, name());
            scanner
                    .withSchemaManager(env.schemaManager())
                    .init(env.agentConfig(), env.connectionManager());
            state.setState(ProcessorState.EProcessorState.Initialized);
            return this;
        } catch (Throwable t) {
            DefaultLogger.error(t.getLocalizedMessage());
            DefaultLogger.stacktrace(t);
            state.error(t);
            throw new NameNodeError(t);
        }
    }

    @Override
    public SchemaScanner start() throws Exception {
        try {
            Preconditions.checkNotNull(scanner);
            Preconditions.checkState(state.getState() == ProcessorState.EProcessorState.Initialized);
            state.setState(ProcessorState.EProcessorState.Running);
            scanner.run();
            state.setState(ProcessorState.EProcessorState.Initialized);
            return this;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(env.LOG, t);
            DefaultLogger.error(env.LOG, t.getLocalizedMessage());
            NameNodeEnv.get(name()).error(t);
            throw t;
        }
    }

    @Override
    public SchemaScanner stop() throws Exception {
        if (!state.hasError()) {
            state.setState(ProcessorState.EProcessorState.Stopped);
        }
        return this;
    }

    @Override
    public AbstractState<ProcessorState.EProcessorState> status() {
        return state;
    }


    @Override
    public String name() {
        return getClass().getSimpleName();
    }

    @Override
    public void checkState() throws Exception {
        if (env == null) {
            throw new Exception(String.format("[%s] Environment is not available...", name()));
        }
        if (!env.state().isAvailable()) {
            throw new Exception(
                    String.format("[%s] Environment state is not valid. [state=%s]",
                            name(), env.state().getState().name()));
        }
        if (state.getState() != ProcessorState.EProcessorState.Initialized) {
            throw new Exception(
                    String.format("[%s] Replicator not available. [state=%s]",
                            name(), state.getState().name()));
        }
    }

    public static void main(String[] args) {
        try {
            SchemaScanner runner = new SchemaScanner();
            JCommander.newBuilder().addObject(runner).build().parse(args);
            runner.init();
            runner.start();
            runner.stop();
        } catch (Exception ex) {
            DefaultLogger.error(ex.getLocalizedMessage());
            DefaultLogger.stacktrace(ex);
        }
    }
}
