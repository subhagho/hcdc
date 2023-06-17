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

package ai.sapper.hcdc.agents.settings;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.io.EncryptionHandler;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.nio.ByteBuffer;

@Getter
@Setter
public class NameNodeFileScannerSettings extends Settings {
    public static class Constants {
        public static final String CONFIG_HDFS_CONN = "hdfs";
        public static final String CONFIG_SHARD_PATH = "shards";
        public static final String CONFIG_SHARD_COUNT = "shards.count";
        public static final String CONFIG_SHARD_ID = "shards.id";
        public static final String CONFIG_THREAD_COUNT = "threads";
        public static final String CONFIG_ENCRYPTOR_CLASS = "encryption.class";
    }

    @Config(name = Constants.CONFIG_HDFS_CONN)
    private String hdfsConnection;
    private HierarchicalConfiguration<ImmutableNode> fsConfig;
    @Config(name = Constants.CONFIG_SHARD_COUNT, required = false, type = Integer.class)
    private int shardCount = 1;
    @Config(name = Constants.CONFIG_SHARD_ID, required = false, type = Integer.class)
    private int shardId = 0;
    @Config(name = Constants.CONFIG_THREAD_COUNT, required = false, type = Integer.class)
    private int threads = 1;
    @Config(name = Constants.CONFIG_ENCRYPTOR_CLASS, required = false, type = Class.class)
    private Class<? extends EncryptionHandler<ByteBuffer, ByteBuffer>> encryptorClass;
}
