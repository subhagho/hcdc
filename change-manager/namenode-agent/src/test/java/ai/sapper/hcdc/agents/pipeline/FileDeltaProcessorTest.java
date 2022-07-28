package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.services.EConfigFileType;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.io.impl.s3.S3FileSystem;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.junit.jupiter.api.Assertions.*;

class FileDeltaProcessorTest {
    private static final String CONFIG_FILE = "src/test/resources/configs/file-delta-agent-0.xml";
    private static final String DEFAULT_BUCKET_NAME = "hcdc";

    @RegisterExtension
    static final S3MockExtension S3_MOCK =
            S3MockExtension.builder().silent().withSecureConnection(false).build();

    @Test
    void run() {
        try {
            final S3Client s3Client = S3_MOCK.createS3ClientV2();

            s3Client.createBucket(CreateBucketRequest.builder().bucket(DEFAULT_BUCKET_NAME).build());

            HierarchicalConfiguration<ImmutableNode> config = ConfigReader.read(CONFIG_FILE, EConfigFileType.File);
            NameNodeEnv.setup(config);

            FileDeltaProcessor processor
                    = new FileDeltaProcessor(NameNodeEnv.stateManager())
                    .withMockFileSystem(new S3Mocker(s3Client));
            processor.init(NameNodeEnv.get().configNode(), NameNodeEnv.connectionManager());
            processor.run(true);
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    public static class S3Mocker implements FileSystem.FileSystemMocker {
        private final S3Client s3Client;

        public S3Mocker(@NonNull S3Client s3Client) {
            this.s3Client = s3Client;
        }

        /**
         * @param config
         * @return
         * @throws Exception
         */
        @Override
        public FileSystem create(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws Exception {
            return (S3FileSystem) new S3FileSystem()
                    .withClient(s3Client)
                    .init(config, FileDeltaProcessor.FileDeltaProcessorConfig.Constants.CONFIG_PATH_FS);
        }
    }
}