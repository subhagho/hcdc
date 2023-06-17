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

package ai.sapper.hcdc.agents.namenode.main;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.agents.main.EditsLogProcessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

class EditsLogProcessorTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-agent.xml";
    @Test
    void runOnce() {
        try {
            EditsLogProcessor runner = new EditsLogProcessor();
            runner.runOnce(__CONFIG_FILE);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}