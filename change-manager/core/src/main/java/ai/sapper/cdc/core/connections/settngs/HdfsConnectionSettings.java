package ai.sapper.cdc.core.connections.settngs;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public class HdfsConnectionSettings {

    @Getter
    @Setter
    public abstract static class HdfsBaseSettings {
        protected String name;
        protected boolean securityEnabled = false;
        protected boolean adminEnabled = false;
        protected Map<String, String> parameters;
    }

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class HdfsSettings extends HdfsBaseSettings {
        private String primaryNameNodeUri;
        private String secondaryNameNodeUri;
    }


    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class HdfsHASettings extends HdfsBaseSettings {
        private String nameService;
        private String failoverProvider;
        private String[][] nameNodeAddresses;
    }
}
