package ai.sapper.hcdc.common.model.services;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PathWithSchema extends PathOrSchema {
    private String schemaStr;
}
