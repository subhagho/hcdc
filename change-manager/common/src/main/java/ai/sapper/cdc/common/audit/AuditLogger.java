package ai.sapper.cdc.common.audit;

import com.google.protobuf.MessageOrBuilder;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.List;

public interface AuditLogger {
    public static final String __CONFIG_PATH = "audit";
    public static final String CONFIG_AUDIT_CLASS = String.format("%s.class", __CONFIG_PATH);

    AuditLogger init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException;

    <T> void audit(@NonNull Class<?> caller, long timestamp, @NonNull T data);

    void audit(@NonNull Class<?> caller, long timestamp, @NonNull MessageOrBuilder data);

    List<AuditRecord<?>> read(@NonNull String logfile, int offset, int batchSize) throws IOException;
}
