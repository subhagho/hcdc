package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.connections.hadoop.HdfsHAConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

public class HdfsConnectionSettings {

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public abstract static class HdfsBaseSettings extends ConnectionSettings {
        @Setting(name = HdfsConnection.HdfsConfig.Constants.CONN_SECURITY_ENABLED, required = false)
        protected boolean securityEnabled = false;
        @Setting(name = HdfsConnection.HdfsConfig.Constants.CONN_ADMIN_CLIENT_ENABLED, required = false)
        protected boolean adminEnabled = false;

        public HdfsBaseSettings() {
            setType(EConnectionType.hadoop);
        }
    }

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class HdfsSettings extends HdfsBaseSettings {
        @Setting(name = HdfsConnection.HdfsConfig.Constants.CONN_PRI_NAME_NODE_URI)
        private String primaryNameNodeUri;
        @Setting(name = HdfsConnection.HdfsConfig.Constants.CONN_SEC_NAME_NODE_URI)
        private String secondaryNameNodeUri;

        public HdfsSettings() {
            setConnectionClass(HdfsConnection.class);
        }

        @Override
        public void validate() throws Exception {
            ConfigReader.checkStringValue(getName(), getClass(), ConnectionConfig.CONFIG_NAME);
            ConfigReader.checkStringValue(getPrimaryNameNodeUri(), getClass(),
                    HdfsConnection.HdfsConfig.Constants.CONN_PRI_NAME_NODE_URI);
        }
    }


    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class HdfsHASettings extends HdfsBaseSettings {
        @Setting(name = HdfsHAConnection.HdfsHAConfig.Constants.DFS_NAME_SERVICES)
        private String nameService;
        @Setting(name = HdfsHAConnection.HdfsHAConfig.Constants.DFS_FAILOVER_PROVIDER)
        private String failoverProvider;
        @Setting(name = HdfsHAConnection.HdfsHAConfig.Constants.DFS_NAME_NODES, parser = HdfsUrlParser.class)
        private String[][] nameNodeAddresses;

        public HdfsHASettings() {
            setConnectionClass(HdfsHAConnection.class);
        }

        @Override
        public void validate() throws Exception {
            ConfigReader.checkStringValue(getName(), getClass(), ConnectionConfig.CONFIG_NAME);
            ConfigReader.checkStringValue(getNameService(), getClass(),
                    HdfsHAConnection.HdfsHAConfig.Constants.DFS_NAME_SERVICES);
            ConfigReader.checkStringValue(getFailoverProvider(), getClass(),
                    HdfsHAConnection.HdfsHAConfig.Constants.DFS_FAILOVER_PROVIDER);
        }
    }
}
