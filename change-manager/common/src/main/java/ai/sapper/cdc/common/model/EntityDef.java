package ai.sapper.cdc.common.model;

import ai.sapper.cdc.common.schema.SchemaVersion;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;

@Getter
@Setter
@Accessors(fluent = true)
public class EntityDef {
    private SchemaVersion version;
    private Schema schema;
    private String schemaPath;
}
