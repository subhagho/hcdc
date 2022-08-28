package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.utils.DefaultLogger;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.List;
import java.util.Properties;

@Getter
@Setter
@Accessors(fluent = true)
public class HadoopEnvConfig {
    public static class Constants {
        private static final String HDFS_CONFIG_PROPERTY = "property";
        private static final String HDFS_CONFIG_PROPERTY_NAME = "name";
        private static final String HDFS_CONFIG_PROPERTY_VALUE = "value";
        private static final String HDFS_NN_DATA_DIR = "dfs.namenode.name.dir";
        private static final String HDFS_NN_EDITS_DIR = "fs.namenode.edits.dir";
        private static final String HDFS_NN_NAMESPACE = "dfs.nameservices";
        private static final String HDFS_NN_NODES = "dfs.ha.namenodes.%s";
        private static final String HDFS_NN_URL = "dfs.namenode.http-address.%s.%s";
    }

    private final String hadoopHome;
    private final String hdfsConfigFile;
    private final String nameNodeInstanceName;
    private final String namespace;
    private HierarchicalConfiguration<ImmutableNode> hdfsConfig;
    private Properties config;
    private String nameNodeDataDir;
    private String nameNodeEditsDir;
    private String nameNodeAdminUrl;
    private short version;

    public HadoopEnvConfig(@NonNull String hadoopHome,
                           @NonNull String hdfsConfigFile,
                           @NonNull String namespace,
                           @NonNull String nameNodeInstanceName,
                           short version) {
        this.hadoopHome = hadoopHome;
        this.hdfsConfigFile = hdfsConfigFile;
        this.nameNodeInstanceName = nameNodeInstanceName;
        this.namespace = namespace;
        this.version = version;
    }

    public HadoopEnvConfig withNameNodeAdminUrl(String nameNodeAdminUrl) {
        this.nameNodeAdminUrl = nameNodeAdminUrl;
        return this;
    }

    public void read() throws Exception {
        File cf = new File(hdfsConfigFile);
        if (!cf.exists()) {
            throw new Exception(String.format("Configuration file not found. ]path=%s]", cf.getAbsolutePath()));
        }
        Configurations configs = new Configurations();
        hdfsConfig = configs.xml(cf);

        List<HierarchicalConfiguration<ImmutableNode>> nodes = hdfsConfig.configurationsAt(Constants.HDFS_CONFIG_PROPERTY);
        if (nodes == null || nodes.isEmpty()) {
            throw new ConfigurationException(String.format("Failed to read HDFS configuration. [file=%s]", cf.getAbsolutePath()));
        }
        config = new Properties();
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            String sn = node.getString(Constants.HDFS_CONFIG_PROPERTY_NAME);
            String vn = node.getString(Constants.HDFS_CONFIG_PROPERTY_VALUE);
            if (!Strings.isNullOrEmpty(sn)) {
                config.put(sn, vn);
            }
        }
        nameNodeDataDir = config.getProperty(Constants.HDFS_NN_DATA_DIR);
        if (Strings.isNullOrEmpty(nameNodeDataDir)) {
            throw new Exception(String.format("HDFS Configuration not found. [name=%s][file=%s]",
                    Constants.HDFS_NN_DATA_DIR, cf.getAbsolutePath()));
        }
        nameNodeEditsDir = nameNodeDataDir;
        String ed = config.getProperty(Constants.HDFS_NN_EDITS_DIR);
        if (!Strings.isNullOrEmpty(ed)) {
            nameNodeEditsDir = ed;
        }
        if (isHAEnabled(config)) {
            readHAConfig(config, cf);
        } else {
            readConfig();
        }
        DefaultLogger.LOGGER.info(String.format("Using NameNode Admin URL [%s]", nameNodeAdminUrl));
    }

    private void readConfig() throws Exception {
        if (Strings.isNullOrEmpty(nameNodeAdminUrl)) {
            throw new ConfigurationException("NameNode Admin URL needs to be set for non-HA connections.");
        }
    }

    private void readHAConfig(Properties config, File cf) throws Exception {
        String ns = namespace;
        ns = config.getProperty(Constants.HDFS_NN_NAMESPACE);
        if (Strings.isNullOrEmpty(ns)) {
            throw new Exception(String.format("HDFS Configuration not found. [name=%s][file=%s]",
                    Constants.HDFS_NN_NAMESPACE, cf.getAbsolutePath()));
        }
        if (ns.compareToIgnoreCase(namespace) != 0) {
            throw new Exception(String.format("HDFS Namespace mismatch. [expected=%s][actual=%s]", ns, namespace));
        }
        String nnKey = String.format(Constants.HDFS_NN_NODES, ns);
        String nns = config.getProperty(nnKey);
        if (Strings.isNullOrEmpty(nns)) {
            throw new Exception(String.format("HDFS Configuration not found. [name=%s][file=%s]", nnKey, cf.getAbsolutePath()));
        }
        String[] parts = nns.split(",");
        String nn = null;
        for (String part : parts) {
            if (part.trim().compareToIgnoreCase(nameNodeInstanceName) == 0) {
                nn = part.trim();
                break;
            }
        }
        if (Strings.isNullOrEmpty(nn)) {
            throw new Exception(
                    String.format("NameNode instance not found in HDFS configuration. [instance=%s][namenodes=%s][file=%s]",
                            nameNodeInstanceName, nns, cf.getAbsolutePath()));
        }
        DefaultLogger.LOGGER.info(String.format("Using NameNode instance [%s.%s]", ns, nn));
        String urlKey = String.format(Constants.HDFS_NN_URL, ns, nn);
        nameNodeAdminUrl = config.getProperty(urlKey);
        if (Strings.isNullOrEmpty(nameNodeAdminUrl)) {
            throw new Exception(String.format("NameNode Admin URL not found. [name=%s][file=%s]", urlKey, cf.getAbsolutePath()));
        }
    }

    private boolean isHAEnabled(Properties config) {
        return config.containsKey(Constants.HDFS_NN_NAMESPACE);
    }
}