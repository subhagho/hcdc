package ai.sapper.cdc.core.model;

import ai.sapper.cdc.common.AbstractState;
import lombok.NonNull;

public class CDCAgentState {
    public enum EAgentState {
        Unknown, Active, StandBy, Error, Stopped
    }

    public static class AgentState extends AbstractState<EAgentState> {

        public AgentState() {
            super(EAgentState.Error);
            state(EAgentState.Unknown);
        }

        public EAgentState parseState(@NonNull String state) {
            EAgentState s = null;
            for (EAgentState ss : EAgentState.values()) {
                if (state.compareToIgnoreCase(ss.name()) == 0) {
                    s = ss;
                    break;
                }
            }
            if (s != null) {
                state(s);
            }
            return s;
        }
    }
}
