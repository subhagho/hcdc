package ai.sapper.cdc.common.model.services;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PathWithSchema extends PathOrSchema {
    private String schemaStr;
    private String version;
}
