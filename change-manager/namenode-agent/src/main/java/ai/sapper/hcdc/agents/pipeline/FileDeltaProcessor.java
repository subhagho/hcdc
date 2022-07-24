package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.messaging.InvalidMessageError;
import ai.sapper.hcdc.core.messaging.MessageObject;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FileDeltaProcessor extends ChangeDeltaProcessor {
    private static Logger LOG = LoggerFactory.getLogger(FileDeltaProcessor.class);
    private FileTransactionProcessor processor;
    private long receiveBatchTimeout = 1000;

    public FileDeltaProcessor(@NonNull ZkStateManager stateManager) {
        super(stateManager);
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        Preconditions.checkState(sender() != null);
        Preconditions.checkState(receiver() != null);
        Preconditions.checkState(errorSender() != null);
        try {
            while (NameNodeEnv.get().state().isAvailable()) {
                List<MessageObject<String, DFSChangeDelta>> batch = receiver().nextBatch(receiveBatchTimeout);
                if (batch == null || batch.isEmpty()) {
                    Thread.sleep(receiveBatchTimeout);
                    continue;
                }
                LOG.debug(String.format("Received messages. [count=%d]", batch.size()));
                for (MessageObject<String, DFSChangeDelta> message : batch) {
                    try {
                        long txId = process(message);
                        if (txId > 0) {
                            if (message.mode() == MessageObject.MessageMode.New) {
                                stateManager().update(txId);
                                LOG.debug(String.format("Processed transaction delta. [TXID=%d]", txId));
                            } else if (message.mode() == MessageObject.MessageMode.Snapshot) {
                                if (stateManager().agentTxState().getProcessedTxId() < txId) {
                                    stateManager().update(txId);
                                    LOG.debug(String.format("Processed transaction delta. [TXID=%d]", txId));
                                }
                            }
                        }
                    } catch (InvalidMessageError ie) {
                        LOG.error("Error processing message.", ie);
                        DefaultLogger.stacktrace(LOG, ie);
                        errorSender().send(message);
                    }
                    receiver().ack(message.id());
                }
            }
            LOG.warn(String.format("Delta Change Processor thread stopped. [env state=%s]", NameNodeEnv.get().state().state().name()));
        } catch (Throwable t) {
            LOG.error("Delta Change Processor terminated with error", t);
            DefaultLogger.stacktrace(LOG, t);
        }
    }

    private long process(MessageObject<String, DFSChangeDelta> message) throws Exception {
        long txId = -1;
        if (!isValidMessage(message)) {
            throw new InvalidMessageError(message.id(),
                    String.format("Invalid Message mode. [id=%s][mode=%s]", message.id(), message.mode().name()));
        }
        txId = processor.checkMessageSequence(message);

        processor.processTxMessage(message, txId);

        return txId;
    }

    private boolean isValidMessage(MessageObject<String, DFSChangeDelta> message) {
        boolean ret = false;
        if (message.mode() != null) {
            ret = (message.mode() == MessageObject.MessageMode.New
                    || message.mode() == MessageObject.MessageMode.Backlog
                    || message.mode() == MessageObject.MessageMode.Snapshot);
        }
        if (ret) {
            ret = message.value().hasTxId();
        }
        return ret;
    }

    public static class FileDeltaProcessorConfig extends ChangeDeltaProcessorConfig {
        public static final String __CONFIG_PATH = "processor.files";

        public FileDeltaProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
