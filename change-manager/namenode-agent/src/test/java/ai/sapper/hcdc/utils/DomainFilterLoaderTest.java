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

package ai.sapper.hcdc.utils;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class DomainFilterLoaderTest {
    private static final String __CONFIG_PATH = "src/test/resources/configs/hcdc-agent.xml";
    private static final String TEST_DOMAIN_FILE = "src/test/resources/test-domain-loader.csv";

    private static XMLConfiguration xmlConfiguration = null;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_PATH, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
    }

    @Test
    void read() {
        try {
            String name = getClass().getSimpleName();
            NameNodeEnv env = NameNodeEnv.setup(name, getClass(), xmlConfiguration);
            DefaultLogger.info(
                    String.format("Name Node Agent environment initialized. [namespace=%s]",
                            env.module()));
            assertNotNull(env.schemaManager());
            HCdcSchemaManager schemaManager = env.schemaManager();

            new DomainFilterLoader().read(TEST_DOMAIN_FILE, schemaManager);
            NameNodeEnv.dispose(name);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}