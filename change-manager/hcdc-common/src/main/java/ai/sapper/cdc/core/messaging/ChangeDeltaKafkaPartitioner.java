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

package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.core.messaging.kafka.KafkaPartitioner;
import ai.sapper.cdc.core.utils.SchemaEntityHelper;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

public class ChangeDeltaKafkaPartitioner implements KafkaPartitioner<DFSChangeDelta> {
    private static final String __CONFIG_PATH = "partitioner.config";
    private static final String CONFIG_PARTITION_COUNT = "partitions";

    private HierarchicalConfiguration<ImmutableNode> config;
    private int partitionCount;

    /**
     * @param xmlConfig
     * @throws ConfigurationException
     */
    @Override
    public void init(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        config = xmlConfig.configurationAt(__CONFIG_PATH);
        String pcount = config.getString(CONFIG_PARTITION_COUNT);
        if (Strings.isNullOrEmpty(pcount)) {
            throw new ConfigurationException(
                    String.format("Partitioner configuration node not found. [path=%s]", CONFIG_PARTITION_COUNT));
        }
        partitionCount = Integer.parseInt(pcount);
    }

    /**
     * @param key
     * @return
     */
    @Override
    public int partition(@NonNull DFSChangeDelta key) {
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(key.getEntity());
        String entity = schemaEntity.getEntity();
        int hash = schemaEntity.getGroup();
        if (schemaEntity.getGroup() < 0) {
            String pk = String.format("%s::%s",
                    schemaEntity.getDomain(), entity);
            hash = pk.hashCode();
        }

        if (hash < 0) {
            hash *= -1;
        }
        return (hash % partitionCount);
    }
}
