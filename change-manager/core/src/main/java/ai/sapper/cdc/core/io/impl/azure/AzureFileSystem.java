package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import ai.sapper.cdc.core.io.impl.local.LocalFileSystem;
import ai.sapper.cdc.core.keystore.KeyStore;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

public class AzureFileSystem extends LocalFileSystem {
    private AzureFsClient client;

    /**
     * @param config
     * @param pathPrefix
     * @return
     * @throws IOException
     */
    @Override
    public CDCFileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              String pathPrefix,
                              KeyStore keyStore) throws IOException {
        try {
            super.init(config, pathPrefix, keyStore);
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
