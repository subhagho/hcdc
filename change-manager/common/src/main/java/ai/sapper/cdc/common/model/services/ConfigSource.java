package ai.sapper.cdc.common.model.services;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ConfigSource {
    private EConfigFileType type;
    private String path;
}
