package ai.sapper.cdc.core.connections.settngs;

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
        protected boolean securityEnabled = false;
        protected boolean adminEnabled = false;
    }

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class HdfsSettings extends HdfsBaseSettings {
        private String primaryNameNodeUri;
        private String secondaryNameNodeUri;

        public HdfsSettings() {
            setConnectionType(HdfsConnection.class);
        }
    }


    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class HdfsHASettings extends HdfsBaseSettings {
        private String nameService;
        private String failoverProvider;
        private String[][] nameNodeAddresses;

        public HdfsHASettings() {
            setConnectionType(HdfsHAConnection.class);
        }
    }
}
