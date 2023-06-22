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
import ai.sapper.cdc.core.Service;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.pipeline.EntityChangeDeltaProcessor;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

@Getter
public class EntityChangeDeltaConsumer implements Service<ProcessorState.EProcessorState> {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    private EConfigFileType fileSource = EConfigFileType.File;
    private HierarchicalConfiguration<ImmutableNode> config;
    private Thread runner;
    private EntityChangeDeltaProcessor<?> processor;
    private NameNodeEnv env;

    @Override
    public EntityChangeDeltaConsumer setConfigFile(@NonNull String path) {
        configFile = path;
        return this;
    }

    @Override
    public EntityChangeDeltaConsumer setConfigSource(@NonNull String type) {
        configSource = type;
        return this;
    }

    @SuppressWarnings("unchecked")
    public EntityChangeDeltaConsumer init() throws Exception {
        try {
            Preconditions.checkState(!Strings.isNullOrEmpty(configFile));
            if (!Strings.isNullOrEmpty(configSource)) {
                fileSource = EConfigFileType.parse(configSource);
            }
            Preconditions.checkNotNull(fileSource);
            config = ConfigReader.read(configFile, fileSource);
            env = NameNodeEnv.setup(name(), getClass(), config);

            ChangeDeltaProcessor.ProcessorDef def = ChangeDeltaProcessor.readProcessorType(env.agentConfig());
            Preconditions.checkNotNull(def);
            if (def.type() == null) {
                throw new Exception("EditsChangeDeltaProcessor implementation not specified...");
            }

            processor = (EntityChangeDeltaProcessor<?>) def.type().getDeclaredConstructor(NameNodeEnv.class, String.class)
                    .newInstance(env, name());
            processor.init(def.name(), env.agentConfig());
            return this;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(t.getLocalizedMessage());
            throw t;
        }
    }

    @Override
    public EntityChangeDeltaConsumer start() throws Exception {
        try {
            if (processor == null || status().getState() != ProcessorState.EProcessorState.Initialized) {
                throw new Exception(
                        String.format("[%s] Processor not initialized. [state=%s]",
                                name(), status().getState().name()));
            }
            if (processor.state().isRunning()) {
                return this;
            }
            processor.state().setState(ProcessorState.EProcessorState.Running);
            runner = new Thread(processor);
            runner.start();
            return this;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(env.LOG, t);
            DefaultLogger.error(env.LOG, t.getLocalizedMessage());
            throw t;
        }
    }

    @Override
    public EntityChangeDeltaConsumer stop() throws Exception {
        if (processor != null) {
            processor.stop();
            if (runner != null)
                runner.join();
        }
        return this;
    }

    @Override
    public AbstractState<ProcessorState.EProcessorState> status() {
        Preconditions.checkNotNull(processor);
        return processor.state();
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
        if (!processor.state().isRunning()) {
            throw new Exception(
                    String.format("[%s] Processor is not running. [state=%s]",
                            name(), processor.state().getState().name()));
        }
    }

    public static void main(String[] args) {
        try {
            EntityChangeDeltaConsumer runner = new EntityChangeDeltaConsumer();
            JCommander.newBuilder().addObject(runner).build().parse(args);
            runner.init();
            runner.start();
        } catch (Throwable t) {
            t.printStackTrace();
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(t.getLocalizedMessage());
        }
    }
}
