package ai.sapper.cdc.common.model.services;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PathOrSchema {
    private String domain;
    private String node;
    private String zkPath;
}
