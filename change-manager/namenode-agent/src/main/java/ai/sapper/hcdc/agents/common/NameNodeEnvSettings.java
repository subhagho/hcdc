package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.BaseEnvSettings;
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
        public static final String CONFIG_CONNECTION_HDFS = "hadoop.hdfs-admin";
        public static final String CONFIG_HADOOP_HOME = "hadoop.home";
        public static final String CONFIG_HADOOP_INSTANCE = "hadoop.instance";
        public static final String CONFIG_HADOOP_NAMESPACE = "hadoop.namespace";
        public static final String CONFIG_HADOOP_VERSION = "hadoop.version";
        public static final String CONFIG_HADOOP_ADMIN_URL = "hadoop.adminUrl";
        public static final String CONFIG_HADOOP_TMP_DIR = "hadoop.tmpDir";

        public static final String CONFIG_HADOOP_CONFIG = "hadoop.config";

        public static final String HDFS_NN_USE_HTTPS = "useSSL";
        public static final String CONFIG_LOAD_HADOOP = "needHadoop";
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
