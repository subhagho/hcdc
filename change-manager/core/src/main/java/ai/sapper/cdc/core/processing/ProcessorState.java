package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.AbstractState;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class ProcessorState extends AbstractState<ProcessorState.EProcessorState> {
    public ProcessorState() {
        super(EProcessorState.Error);
        state(EProcessorState.Unknown);
    }

    @JsonIgnore
    public boolean isInitialized() {
        return (state() == EProcessorState.Initialized || state() == EProcessorState.Running);
    }

    @JsonIgnore
    public boolean isRunning() {
        return (state() == EProcessorState.Running);
    }

    public enum EProcessorState {
        Unknown, Initialized, Running, Stopped, Error, Paused;
    }
}
