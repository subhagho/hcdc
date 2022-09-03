package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.audit.AuditLogger;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.kafka.BasicKafkaConsumer;
import ai.sapper.cdc.core.connections.kafka.BasicKafkaProducer;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class HCDCMessagingBuilders {
    public enum EConnectionType {
        Unknown, Kafka;

        public static EConnectionType parse(@NonNull String type) {
            for (EConnectionType t : EConnectionType.values()) {
                if (t.name().compareToIgnoreCase(type) == 0) {
                    return t;
                }
            }
            return null;
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class SenderBuilder {
        private String type;
        private String connection;
        private String partitioner;
        private HierarchicalConfiguration<ImmutableNode> config;
        private ConnectionManager manager;
        private AuditLogger auditLogger;

        public MessageSender<String, DFSChangeDelta> build() throws MessagingError {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(type));
            Preconditions.checkArgument(!Strings.isNullOrEmpty(connection));

            EConnectionType ct = EConnectionType.parse(type);
            if (ct == null || ct == EConnectionType.Unknown) {
                throw new MessagingError(String.format("Connection type not supported. [type=%s]", type));
            }
            if (ct == EConnectionType.Kafka) {
                return buildKafka();
            }
            throw new MessagingError(String.format("Connection type not implemented. [type=%s]", ct.name()));
        }

        private MessageSender<String, DFSChangeDelta> buildKafka() throws MessagingError {
            try {
                BasicKafkaProducer kc = manager.getConnection(connection, BasicKafkaProducer.class);
                if (kc == null) {
                    throw new MessagingError(String.format("Kafka Connection not found. [name=%s]", connection));
                }
                if (!kc.isConnected()) {
                    kc.connect();
                }
                KafkaPartitioner<DFSChangeDelta> part = null;
                if (!Strings.isNullOrEmpty(partitioner)) {
                    Class<? extends KafkaPartitioner<DFSChangeDelta>> cp = (Class<? extends KafkaPartitioner<DFSChangeDelta>>) Class.forName(partitioner);
                    part = cp.newInstance();
                    if (config != null) {
                        part.init(config);
                    }
                }

                return new HCDCKafkaSender()
                        .withPartitioner(part)
                        .withConnection(kc)
                        .withAuditLogger(auditLogger);
            } catch (MessagingError me) {
                throw me;
            } catch (Exception ex) {
                throw new MessagingError(ex);
            }
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ReceiverBuilder {
        private String type;
        private String connection;
        private ZookeeperConnection zkConnection;
        private String zkStatePath;
        private boolean saveState = false;

        private HierarchicalConfiguration<ImmutableNode> config;
        private ConnectionManager manager;
        private int batchSize = -1;
        private AuditLogger auditLogger;

        public MessageReceiver<String, DFSChangeDelta> build() throws MessagingError {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(type));
            Preconditions.checkArgument(!Strings.isNullOrEmpty(connection));

            try {
                EConnectionType ct = EConnectionType.parse(type);
                if (ct == null || ct == EConnectionType.Unknown) {
                    throw new MessagingError(String.format("Connection type not supported. [type=%s]", type));
                }
                if (ct == EConnectionType.Kafka) {
                    return buildKafka().init();
                }
                throw new MessagingError(String.format("Connection type not implemented. [type=%s]", ct.name()));
            } catch (MessagingError me) {
                throw me;
            } catch (Exception ex) {
                throw new MessagingError(ex);
            }
        }

        private MessageReceiver<String, DFSChangeDelta> buildKafka() throws Exception {
            BasicKafkaConsumer kc = manager.getConnection(connection, BasicKafkaConsumer.class);
            if (kc == null) {
                throw new MessagingError(String.format("Kafka Connection not found. [name=%s]", connection));
            }
            if (!kc.isConnected()) {
                kc.connect();
            }
            return new HCDCKafkaReceiver()
                    .withConnection(kc)
                    .withSaveState(saveState)
                    .withZkPath(zkStatePath)
                    .withZookeeperConnection(zkConnection)
                    .withAuditLogger(auditLogger);
        }
    }
}
