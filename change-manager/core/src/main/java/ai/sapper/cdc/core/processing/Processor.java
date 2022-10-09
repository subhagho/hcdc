package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseStateManager;
import ai.sapper.cdc.core.model.Heartbeat;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class Processor<S, T> implements Runnable, Closeable {
    public static class Constants {
        public static final String CONFIG_PATH = "processor";
        public static final String CONFIG_CONSUMER_PATH = "consumer";
        public static final String CONFIG_PRODUCER_PATH = "producer";
        public static final String CONFIG_ERRORS_PATH = "errors";
        public static final String CONFIG_CONSUMER_CLASS = String.format("%s.class", CONFIG_CONSUMER_PATH);
        public static final String CONFIG_PRODUCER_CLASS = String.format("%s.class", CONFIG_PRODUCER_PATH);
        public static final String CONFIG_ERRORS_CLASS = String.format("%s.class", CONFIG_ERRORS_PATH);
        public static final long THREAD_SLEEP_TIMEOUT = 500;
    }

    private TransactionConsumer<S> consumer;
    private TransactionProducer<T> producer;
    private TransactionProducer<T> errors;
    private final BaseStateManager stateManager;
    private final String name;
    private final ProcessorState state = new ProcessorState();
    private HierarchicalConfiguration<ImmutableNode> config;

    public Processor(@NonNull BaseStateManager stateManager,
                     @NonNull String name) {
        this.stateManager = stateManager;
        this.name = name;
    }

    @SuppressWarnings("unchecked")
    public Processor<S, T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig)
            throws ConfigurationException {
        try {
            config = xmlConfig.configurationAt(Constants.CONFIG_PATH);
            // Initialize Consumer
            String c = config.getString(Constants.CONFIG_CONSUMER_CLASS);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(c));
            Class<? extends TransactionConsumer<S>> cCls = (Class<? extends TransactionConsumer<S>>) Class.forName(c);
            consumer = cCls.getDeclaredConstructor().newInstance();
            HierarchicalConfiguration<ImmutableNode> con = config.configurationAt(Constants.CONFIG_CONSUMER_PATH);
            consumer.init(con);
            // Initialize Producer
            c = config.getString(Constants.CONFIG_PRODUCER_CLASS);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(c));
            Class<? extends TransactionProducer<T>> pCls = (Class<? extends TransactionProducer<T>>) Class.forName(c);
            producer = pCls.getDeclaredConstructor().newInstance();
            con = config.configurationAt(Constants.CONFIG_PRODUCER_PATH);
            producer.init(con);
            // Check and initialize Error processor
            if (ConfigReader.checkIfNodeExists(config, Constants.CONFIG_ERRORS_PATH)) {
                c = config.getString(Constants.CONFIG_ERRORS_CLASS);
                Preconditions.checkArgument(!Strings.isNullOrEmpty(c));
                pCls = (Class<? extends TransactionProducer<T>>) Class.forName(c);
                errors = pCls.getDeclaredConstructor().newInstance();
                con = config.configurationAt(Constants.CONFIG_ERRORS_PATH);
                errors.init(con);
            }
            state.state(ProcessorState.EProcessorState.Initialized);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }
        if (errors != null) {
            errors.close();
            errors = null;
        }
    }

    @Override
    public void run() {
        if (!state.isInitialized()) {
            DefaultLogger.LOGGER.error(String.format("Processor not initialized. [state=%s]", state.state().name()));
            return;
        }
        try {
            try {
                state.state(ProcessorState.EProcessorState.Running);
                while (state.isRunning()) {
                    Heartbeat hb = stateManager.heartbeat(name, state.state().name());
                    DefaultLogger.LOGGER.info(hb.toString());
                    List<S> records = consumer.read();
                    if (records == null || records.isEmpty()) {
                        Thread.sleep(Constants.THREAD_SLEEP_TIMEOUT);
                        continue;
                    }
                    for (S record : records) {
                        process(record);
                    }
                }
            } finally {
                close();
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
            state.error(ex);
        }
        DefaultLogger.LOGGER.warn(
                String.format("[MODULE = {%s}] terminated. [state=%s]",
                        stateManager.moduleInstance(), state.state().name()));
    }

    public abstract void process(@NonNull S record) throws Exception;

    public ProcessorState.EProcessorState stop() throws Exception {
        if (state.isRunning()) {
            state.state(ProcessorState.EProcessorState.Stopped);
        }
        return state.state();
    }
}
