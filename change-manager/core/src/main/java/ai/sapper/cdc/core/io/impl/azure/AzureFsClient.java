package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.db.AzureTableConnection;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class AzureFsClient {
    private BlobServiceClient client;
    private AzureFsClientConfig config;
    private AzureStorageAuth auth;

    public AzureFsClient init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull String pathPrefix) throws IOException {
        try {
            config = new AzureFsClientConfig(xmlConfig, pathPrefix);
            config.read();

            BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                    .endpoint(config().endpoint);
            client = auth.credentials(builder).buildClient();
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class AzureFsClientConfig extends ConfigReader {
        private String endpoint;
        private String authClass;
        private String authAccount;

        public AzureFsClientConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                   @NonNull String path) {
            super(config, path);
        }

        public void read() throws ConfigurationException {

        }
    }
}
