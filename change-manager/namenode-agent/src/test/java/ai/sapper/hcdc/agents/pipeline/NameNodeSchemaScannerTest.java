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

package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.NameNodeEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

class NameNodeSchemaScannerTest {
    private static final String CONFIG_FILE = "src/test/resources/configs/hdfs-files-scanner.xml";

    @Test
    void run() {
        try {
            HierarchicalConfiguration<ImmutableNode> config = ConfigReader.read(CONFIG_FILE, EConfigFileType.File);
            String name = NameNodeSchemaScanner.class.getSimpleName();
            NameNodeEnv.setup(name, getClass(), config);

            Preconditions.checkNotNull(NameNodeEnv.get(name).schemaManager());
            NameNodeSchemaScanner scanner = new NameNodeSchemaScanner(NameNodeEnv.get(name), name);
            scanner
                    .withSchemaManager(NameNodeEnv.get(name).schemaManager())
                    .init(NameNodeEnv.get(name).baseConfig(), NameNodeEnv.get(name).connectionManager());
            scanner.run();

            NameNodeEnv.dispose(name);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}