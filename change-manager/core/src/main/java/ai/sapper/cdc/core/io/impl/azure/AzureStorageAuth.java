package ai.sapper.cdc.core.io.impl.azure;

import com.azure.storage.blob.BlobServiceClientBuilder;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

public interface AzureStorageAuth {
    AzureStorageAuth init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                          String pathPrefix) throws IOException;

    BlobServiceClientBuilder credentials(@NonNull BlobServiceClientBuilder builder) throws Exception;
}
