package ai.sapper.cdc.core.io.impl.glacier;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.ChecksumUtils;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.PathInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glacier.GlacierClient;
import software.amazon.awssdk.services.glacier.model.UploadArchiveRequest;
import software.amazon.awssdk.services.glacier.model.UploadArchiveResponse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class GlacierArchiver extends Archiver {
    private GlacierClient client;
    private GlacierArchiverConfig config;

    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig, String pathPrefix) throws IOException {
        try {
            config = new GlacierArchiverConfig(xmlConfig);
            config.read();

            Region region = Region.of(config.region);
            client = GlacierClient.builder()
                    .region(region)
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .build();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public PathInfo archive(@NonNull PathInfo source, @NonNull PathInfo target, @NonNull FileSystem sourceFS) throws IOException {
        Preconditions.checkArgument(target instanceof GlacierPathInfo);
        File zip = new File(source.path());
        try {
            String checkVal = ChecksumUtils.computeSHA256(zip);

            UploadArchiveRequest uploadRequest = UploadArchiveRequest.builder()
                    .vaultName(((GlacierPathInfo) target).vault())
                    .checksum(checkVal)
                    .build();

            UploadArchiveResponse res = client.uploadArchive(uploadRequest, Paths.get(zip.getAbsolutePath()));
            String id = res.archiveId();
            return new GlacierPathInfo(client, id, target.domain(), ((GlacierPathInfo) target).vault());
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public PathInfo getTargetPath(@NonNull String path, @NonNull SchemaEntity schemaEntity) {
        String vault = config.defaultVault;
        if (config.mappings != null && config.mappings.containsKey(schemaEntity.getDomain())) {
            vault = config.mappings.get(schemaEntity.getDomain());
        }
        path = String.format("%s/%s", schemaEntity.getDomain(), path);
        return new GlacierPathInfo(client, path, schemaEntity.getDomain(), vault);
    }

    @Override
    public File getFromArchive(@NonNull PathInfo path) throws IOException {
        throw new IOException("Method not implemented...");
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class GlacierArchiverConfig extends ConfigReader {
        public static class Constants {
            public static final String CONFIG_REGION = "region";
            public static final String CONFIG_DEFAULT_VAULT = "defaultVault";
            public static final String CONFIG_DOMAIN_MAP = "domains.mapping";
        }

        private String region;
        private String defaultVault;

        private Map<String, String> mappings;

        public GlacierArchiverConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, Archiver.CONFIG_ARCHIVER);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Glacier Archiver Configuration not set or is NULL");
            }
            region = get().getString(Constants.CONFIG_REGION);
            if (Strings.isNullOrEmpty(region)) {
                throw new ConfigurationException(
                        String.format("Glacier Archiver Configuration: missing parameter. [name=%s]",
                                Constants.CONFIG_REGION));
            }
            defaultVault = get().getString(Constants.CONFIG_DEFAULT_VAULT);
            if (Strings.isNullOrEmpty(defaultVault)) {
                throw new ConfigurationException(
                        String.format("Glacier Archiver Configuration: missing parameter. [name=%s]",
                                Constants.CONFIG_DEFAULT_VAULT));
            }
            if (ConfigReader.checkIfNodeExists(get(), Constants.CONFIG_DOMAIN_MAP)) {
                mappings = ConfigReader.readAsMap(get(), Constants.CONFIG_DOMAIN_MAP);
            }
        }
    }
}
