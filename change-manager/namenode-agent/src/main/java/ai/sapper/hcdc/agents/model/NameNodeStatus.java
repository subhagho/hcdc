package ai.sapper.hcdc.agents.model;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class NameNodeStatus {
    public static class Contants {
        public static final String KEY_HEADER = "name";
        public static final String KEY_M_TYPE = "modelerType";
        public static final String KEY_STATE = "State";
        public static final String KEY_HOST = "HostAndPort";
        public static final String KEY_ROLE = "NNRole";
        public static final String KEY_SEC_ENABLED = "SecurityEnabled";
        public static final String KEY_HA_TRANSITION_TIME = "LastHATransitionTime";
    }

    private String header;
    private String modelerType;
    private String state;
    private String nNRole;
    private String host;
    private boolean securityEnabled;
    private long haLastTransitionTime;

    public NameNodeStatus parse(@NonNull Map<String, String> map) {
        header = map.get(Contants.KEY_HEADER);
        modelerType = map.get(Contants.KEY_M_TYPE);
        state = map.get(Contants.KEY_STATE);
        nNRole = map.get(Contants.KEY_ROLE);
        host = map.get(Contants.KEY_HOST);
        String s = map.get(Contants.KEY_SEC_ENABLED);
        if (!Strings.isNullOrEmpty(s)) {
            securityEnabled = Boolean.parseBoolean(s);
        }
        s = map.get(Contants.KEY_HA_TRANSITION_TIME);
        if (!Strings.isNullOrEmpty(s)) {
            haLastTransitionTime = Long.parseLong(s);
        }
        return this;
    }
}
