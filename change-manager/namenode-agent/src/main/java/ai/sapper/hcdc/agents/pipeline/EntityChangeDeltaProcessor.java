package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.model.DFSFileState;
import ai.sapper.hcdc.agents.model.EFileState;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSTransaction;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Getter
@Accessors(fluent = true)
public class EntityChangeDeltaProcessor extends ChangeDeltaProcessor {
    private static Logger LOG = LoggerFactory.getLogger(EntityChangeDeltaProcessor.class.getCanonicalName());


    public EntityChangeDeltaProcessor(@NonNull ZkStateManager stateManager, @NonNull String name) {
        super(stateManager, name, EProcessorMode.Reader, true);
    }

    public ChangeDeltaProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                           @NonNull ConnectionManager manger) throws ConfigurationException {
        ChangeDeltaProcessorConfig config = new CDCChangeDeltaProcessorConfig(xmlConfig);
        super.init(config, manger);
        EntityChangeTransactionProcessor processor = (EntityChangeTransactionProcessor) new EntityChangeTransactionProcessor(name())
                .withSenderQueue(sender())
                .withStateManager(stateManager())
                .withErrorQueue(errorSender());
        return withProcessor(processor);
    }

    public void cleanFileState() throws Exception {
        Preconditions.checkState(sender() != null);
        Preconditions.checkState(receiver() != null);
        Preconditions.checkState(errorSender() != null);

        List<DFSFileState> files = stateManager().fileStateHelper().listFiles(null, EFileState.Deleted);
        if (files != null && !files.isEmpty()) {
            for (DFSFileState file : files) {
                DFSFileState f = stateManager().fileStateHelper().delete(file.getFileInfo().getHdfsPath());
                if (f != null) {
                    LOG.debug(String.format("File node deleted. [path=%s]", f.getFileInfo().getHdfsPath()));
                } else {
                    LOG.error(String.format("Failed to delete file node. [path=%s]", file.getFileInfo().getHdfsPath()));
                }
            }
        }
    }

    public boolean isValidMessage(@NonNull MessageObject<String, DFSChangeDelta> message) {
        boolean ret = false;
        if (message.mode() != null) {
            ret = (message.mode() == MessageObject.MessageMode.New
                    || message.mode() == MessageObject.MessageMode.Backlog
                    || message.mode() == MessageObject.MessageMode.Snapshot
                    || message.mode() == MessageObject.MessageMode.Forked
                    || message.mode() == MessageObject.MessageMode.Recursive);
        }
        if (ret) {
            ret = message.value().hasTxId();
        }
        return ret;
    }

    @Override
    public void batchStart() throws Exception {

    }

    @Override
    public void batchEnd() throws Exception {

    }

    @Override
    public void process(@NonNull MessageObject<String, DFSChangeDelta> message,
                        @NonNull Object data,
                        DFSTransaction tnx,
                        boolean retry) throws Exception {
        EntityChangeTransactionProcessor processor
                = (EntityChangeTransactionProcessor) processor();
        processor.processTxMessage(message, data, tnx, retry);
    }

    public static class CDCChangeDeltaProcessorConfig extends ChangeDeltaProcessorConfig {
        public static final String __CONFIG_PATH = "processor.cdc";

        public CDCChangeDeltaProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
