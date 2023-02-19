package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.model.Context;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

public interface EncryptionHandler<I, O> {
    String __CONFIG_PATH = "encryption";

    EncryptionHandler<I, O> init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException;

    O encrypt(@NonNull I input, Context context) throws IOException;

    I decrypt(@NonNull O input, Context context) throws IOException;
}
