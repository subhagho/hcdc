package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.hcdc.common.model.DFSSchemaEntity;
import lombok.NonNull;

public class SchemaEntityHelper {
    public static DFSSchemaEntity proto(@NonNull SchemaEntity schemaEntity) {
        DFSSchemaEntity.Builder builder = DFSSchemaEntity.newBuilder()
                .setDomain(schemaEntity.getDomain())
                .setEntity(schemaEntity.getEntity())
                .setSchema(schemaEntity.getSchema());
        return builder.build();
    }

    public static SchemaEntity parse(@NonNull DFSSchemaEntity schema) {
        SchemaEntity se = new SchemaEntity();
        se.setDomain(schema.getDomain());
        se.setEntity(schema.getEntity());
        if (schema.hasSchema())
            se.setSchema(schema.getSchema());
        return se;
    }
}
