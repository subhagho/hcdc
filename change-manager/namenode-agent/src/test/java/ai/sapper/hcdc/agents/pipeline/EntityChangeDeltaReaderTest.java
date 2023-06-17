/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.impl.s3.S3FileSystem;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.junit.jupiter.api.Assertions.fail;

class EntityChangeDeltaReaderTest {
    private static final String CONFIG_FILE = "src/test/resources/configs/file-delta-agent-0.xml";
    private static final String DEFAULT_BUCKET_NAME = "hcdc";

    @RegisterExtension
    static final S3MockExtension S3_MOCK =
            S3MockExtension.builder()
                    .silent()
                    .withSecureConnection(false)
                    .withRootFolder(String.format("%s/s3mock/", System.getProperty("java.io.tmpdir")))
                    .build();

    @Test
    void run() {
        try {
            String name = EntityChangeDeltaReader.class.getSimpleName();
            final S3Client s3Client = S3_MOCK.createS3ClientV2();

            s3Client.createBucket(CreateBucketRequest.builder().bucket(DEFAULT_BUCKET_NAME).build());

            HierarchicalConfiguration<ImmutableNode> config = ConfigReader.read(CONFIG_FILE, EConfigFileType.File);
            NameNodeEnv.setup(name, getClass(), config);

            EntityChangeDeltaReader processor
                    = new EntityChangeDeltaReader(NameNodeEnv.get(name))
                    .withMockFileSystem(new S3Mocker(s3Client));
            processor.init(NameNodeEnv.get(name).baseConfig());
            processor.run();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
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
        public FileSystem create(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                 @NonNull BaseEnv<?> env) throws Exception {
            S3FileSystem fs = new S3FileSystem()
                    .withClient(s3Client);
            fs.init(config,
                    env,
                    new S3FileSystem.S3FileSystemConfigReader(config));
            return fs;
        }
    }
}