package ai.sapper.cdc.entity.manager;

import ai.sapper.cdc.common.config.Config;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class HCdcSchemaManagerSettings extends SchemaManagerSettings {
    @Config(name = "hdfs")
    private String hdfsConnection;
    @Config(name = "ignore", required = false)
    private String ignoreRegEx;
}
