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
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.EncryptionHandler;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.nio.ByteBuffer;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class EntityChangeDeltaReaderSettings extends ChangeDeltaProcessorSettings {
    public static final String __CONFIG_PATH = "processor.cdc";
    public static class Constants {
        public static final String CONFIG_HDFS_CONN = "hdfs";
        public static final String CONFIG_WS_PATH = "snapshot";
        public static final String CONFIG_ARCHIVER_CLASS = "archiver.class";
        public static final String CONFIG_ENCRYPTOR_CLASS = "encryption.class";
        public static final String CONFIG_FILE_SYSTEM = "fileSystem";
    }

    @Config(name = Constants.CONFIG_HDFS_CONN)
    private String hdfsConnection;
    @Config(name = Constants.CONFIG_FILE_SYSTEM)
    private String fs;
    @Config(name = Constants.CONFIG_ARCHIVER_CLASS, required = false, type = Class.class)
    private Class<? extends Archiver> archiverClass;
    private HierarchicalConfiguration<ImmutableNode> fsConfig;
    @Config(name = Constants.CONFIG_ENCRYPTOR_CLASS, required = false, type = Class.class)
    private Class<? extends EncryptionHandler<ByteBuffer, ByteBuffer>> encryptorClass;
}
