package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.storage.blob.BlobServiceClientBuilder;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

public interface AzureStorageAuth {
    AzureStorageAuth withAccount(@NonNull String account);

    AzureStorageAuth init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                          @NonNull KeyStore keyStore) throws IOException;

    BlobServiceClientBuilder credentials(@NonNull BlobServiceClientBuilder builder) throws Exception;
}
