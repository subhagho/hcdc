package ai.sapper.hcdc.agents.namenode.model;

import ai.sapper.hcdc.common.AbstractState;

public class NameNodeAgentState {
    public enum EAgentState {
        Unknown, Active, StandBy, Error, Stopped
    }

    public static class AgentState extends AbstractState<EAgentState> {

        public AgentState() {
            super(EAgentState.Error);
            state(EAgentState.Unknown);
        }
    }
}
