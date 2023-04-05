package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

public class SharedKeyAuth implements AzureStorageAuth {
    public static final String CONFIG_AUTH_KEY = "authKey";

    private String account;
    private StorageSharedKeyCredential credential;

    @Override
    public AzureStorageAuth withAccount(@NonNull String account) {
        this.account = account;
        return this;
    }

    @Override
    public AzureStorageAuth init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                 @NonNull KeyStore keyStore) throws IOException {
        Preconditions.checkState(!Strings.isNullOrEmpty(account));
        try {
            String key = config.getString(CONFIG_AUTH_KEY);
            if (Strings.isNullOrEmpty(key)) {
                throw new IOException(String.format("Azure Auth key not found. [name=%s]", CONFIG_AUTH_KEY));
            }
            credential = new StorageSharedKeyCredential(account, keyStore.read(key));
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public BlobServiceClientBuilder credentials(@NonNull BlobServiceClientBuilder builder) throws Exception {
        Preconditions.checkNotNull(credential);
        return builder.credential(credential);
    }
}
