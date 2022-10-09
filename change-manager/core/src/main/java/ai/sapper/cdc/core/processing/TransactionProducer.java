package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.model.Context;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;

public interface TransactionProducer<T> extends Closeable {
    void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    void write(@NonNull T record) throws Exception;
}
