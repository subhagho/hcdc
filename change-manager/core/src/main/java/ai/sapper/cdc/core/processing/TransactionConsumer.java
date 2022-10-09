package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.model.Context;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.util.List;

public interface TransactionConsumer<T> extends Closeable {
    void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    List<T> read() throws Exception;
}
