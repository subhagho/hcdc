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

package ai.sapper.cdc.core;

import ai.sapper.cdc.common.config.Config;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.io.File;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class NameNodeEnvSettings extends BaseEnvSettings {
    public static class Constants {
        public static final String __CONFIG_PATH = "agent";

        public static final String CONFIG_SOURCE = "source";
        public static final String CONFIG_CONNECTION_HDFS = "agent.hadoop.hdfs-admin";
        public static final String CONFIG_HADOOP_HOME = "agent.hadoop.home";
        public static final String CONFIG_HADOOP_INSTANCE = "agent.hadoop.instance";
        public static final String CONFIG_HADOOP_NAMESPACE = "agent.hadoop.namespace";
        public static final String CONFIG_HADOOP_VERSION = "agent.hadoop.version";
        public static final String CONFIG_HADOOP_ADMIN_URL = "agent.hadoop.adminUrl";
        public static final String CONFIG_HADOOP_TMP_DIR = "agent.hadoop.tmpDir";

        public static final String CONFIG_HADOOP_CONFIG = "agent.hadoop.config";

        public static final String HDFS_NN_USE_HTTPS = "agent.useSSL";
        public static final String CONFIG_LOAD_HADOOP = "agent.needHadoop";
    }

    @Config(name = Constants.CONFIG_SOURCE)
    private String source;
    @Config(name = Constants.CONFIG_CONNECTION_HDFS, required = false)
    private String hdfsAdminConnection;
    @Config(name = Constants.CONFIG_HADOOP_NAMESPACE, required = false)
    private String hadoopNamespace;
    @Config(name = Constants.CONFIG_HADOOP_INSTANCE, required = false)
    private String hadoopInstanceName;
    @Config(name = Constants.CONFIG_HADOOP_HOME, required = false)
    private String hadoopHome;
    @Config(name = Constants.CONFIG_HADOOP_ADMIN_URL, required = false)
    private String hadoopAdminUrl;
    @Config(name = Constants.CONFIG_HADOOP_CONFIG, required = false)
    private String hadoopConfFile;
    @JsonIgnore
    private File hadoopConfig;
    @Config(name = Constants.HDFS_NN_USE_HTTPS, required = false, type = Boolean.class)
    private boolean hadoopUseSSL = true;
    @Config(name = Constants.CONFIG_LOAD_HADOOP, required = false, type = Boolean.class)
    private boolean readHadoopConfig = true;
    @Config(name = Constants.CONFIG_HADOOP_VERSION, required = false, type = Short.class)
    private short hadoopVersion = 2;
    @Config(name = Constants.CONFIG_HADOOP_TMP_DIR, required = false)
    private String hdfsTmpDir = "/tmp/";
}
