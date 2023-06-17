/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.hcdc.common.model.DFSSchemaEntity;
import lombok.NonNull;

public class SchemaEntityHelper {
    public static DFSSchemaEntity proto(@NonNull SchemaEntity schemaEntity) {
        DFSSchemaEntity.Builder builder = DFSSchemaEntity.newBuilder()
                .setDomain(schemaEntity.getDomain())
                .setEntity(schemaEntity.getEntity())
                .setGroup(schemaEntity.getGroup());
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
