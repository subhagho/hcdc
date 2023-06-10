package ai.sapper.hcdc.agents.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class EntityChangeDeltaProcessorSettings extends ChangeDeltaProcessorSettings {
}
