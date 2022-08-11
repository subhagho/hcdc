package ai.sapper.hcdc.common.model.services;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;

@Getter
@Setter
public class PathWithSchema extends PathOrSchema {
    private String schemaStr;
    private String version;
}
