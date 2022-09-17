package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.hcdc.common.model.DFSSchemaEntity;
import com.google.common.base.Strings;
import lombok.NonNull;

public class SchemaEntityHelper {
    public static DFSSchemaEntity proto(@NonNull SchemaEntity schemaEntity) {
        DFSSchemaEntity.Builder builder = DFSSchemaEntity.newBuilder()
                .setDomain(schemaEntity.getDomain())
                .setEntity(schemaEntity.getEntity());
        if (!Strings.isNullOrEmpty(schemaEntity.getGroup())) {
            builder.setGroup(schemaEntity.getGroup());
        }
        return builder.build();
    }

    public static SchemaEntity parse(@NonNull DFSSchemaEntity schema) {
        SchemaEntity se = new SchemaEntity();
        se.setDomain(schema.getDomain());
        se.setEntity(schema.getEntity());
        if (schema.hasGroup())
            se.setGroup(schema.getGroup());
        return se;
    }
}