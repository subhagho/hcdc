package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.hcdc.common.model.DFSSchema;
import com.google.common.base.Strings;
import lombok.NonNull;

public class SchemaEntityHelper {
    public static DFSSchema proto(@NonNull SchemaEntity schemaEntity) {
        DFSSchema.Builder builder = DFSSchema.newBuilder()
                .setDomain(schemaEntity.getDomain())
                .setEntity(schemaEntity.getEntity());
        if (!Strings.isNullOrEmpty(schemaEntity.getGroup())) {
            builder.setGroup(schemaEntity.getGroup());
        }
        return builder.build();
    }

    public static SchemaEntity parse(@NonNull DFSSchema schema) {
        SchemaEntity se = new SchemaEntity();
        se.setDomain(schema.getDomain());
        se.setEntity(schema.getEntity());
        if (schema.hasGroup()) {
            se.setGroup(schema.getGroup());
        }
        return se;
    }
}
