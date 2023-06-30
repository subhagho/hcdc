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

package ai.sapper.cdc.entity.manager;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import ai.sapper.cdc.entity.manager.zk.ZKSchemaDataHandler;
import ai.sapper.cdc.entity.manager.zk.model.ZkEntitySchema;
import ai.sapper.cdc.entity.schema.EntitySchema;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.entity.schema.SchemaVersion;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;

public class HCdcZkSchemaHandler extends ZKSchemaDataHandler {

    public SchemaVersion currentVersion(SchemaEntity schemaEntity) throws Exception {
        CuratorFramework client = zkConnection().client();
        SchemaVersion version = new SchemaVersion();
        String path = getSchemaPath(schemaEntity.getDomain(), schemaEntity.getEntity())
                .build();
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                version = JSONUtils.read(data, SchemaVersion.class);
            }
        }
        return version;
    }

    public SchemaVersion currentVersion(String path) throws Exception {
        CuratorFramework client = zkConnection().client();
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                return JSONUtils.read(data, SchemaVersion.class);
            }
        }
        return null;
    }

    public List<AvroEntitySchema> findSchemas(@NonNull SchemaEntity entity) throws Exception {
        CuratorFramework client = zkConnection().client();
        String basePath = getSchemaPath(entity.getDomain(), entity.getEntity())
                .build();
        if (client.checkExists().forPath(basePath) != null) {
            List<AvroEntitySchema> schemas = new ArrayList<>();
            List<String> mjvers = client.getChildren().forPath(basePath);
            if (mjvers != null && !mjvers.isEmpty()) {
                for (String mj : mjvers) {
                    int mjv = Integer.parseInt(mj);
                    String path = new PathUtils.ZkPathBuilder(basePath)
                            .withPath(mj)
                            .build();
                    List<String> mnvers = client.getChildren().forPath(path);
                    if (mnvers != null && !mnvers.isEmpty()) {
                        for (String mn : mnvers) {
                            int mnv = Integer.parseInt(mn);
                            String p = new PathUtils.ZkPathBuilder(path)
                                    .withPath(mn)
                                    .build();
                            SchemaVersion v = new SchemaVersion(mjv, mnv);
                            EntitySchema schema = fetchSchema(entity, p);
                            if (schema instanceof AvroEntitySchema) {
                                Preconditions.checkState(schema.getVersion().equals(v));
                                schema.load();
                                schemas.add((AvroEntitySchema) schema);
                            } else {
                                throw new Exception(
                                        String.format("Invalid Schema type. [type=%s][path=%s]",
                                                (schema == null ? "NULL" : schema.getClass().getCanonicalName()), p));
                            }
                        }
                    }
                }
            }
            if (!schemas.isEmpty()) return schemas;
        }
        return null;
    }
}
