package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.state.BaseStateManagerSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class HCdcStateManagerSettings extends BaseStateManagerSettings {
    @Config(name = "source")
    private String source;
}
