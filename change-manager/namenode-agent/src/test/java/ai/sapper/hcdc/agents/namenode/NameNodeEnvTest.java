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

package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.NameNodeEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NameNodeEnvTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-agent.xml";

    private static XMLConfiguration xmlConfiguration = null;
    private static int lockCounter = 0;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
    }

    @Test
    void init() {
        try {
            String name = getClass().getSimpleName();

            NameNodeEnv.setup(name, getClass(), xmlConfiguration);
            DefaultLogger.info(String.format("Name Node Agent environment initialized. [source=%s]",
                    NameNodeEnv.get(name).source()));
            assertNotNull(NameNodeEnv.get(name).hdfsConnection());
            assertNotNull(NameNodeEnv.get(name).stateManager().connection());

            NameNodeEnv.ENameNodeEnvState state = NameNodeEnv.dispose(name);
            assertEquals(NameNodeEnv.ENameNodeEnvState.Disposed, state);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }

    @Test
    void testLocking() {
        try {
            String name = getClass().getSimpleName();

            NameNodeEnv.setup(name, getClass(), xmlConfiguration);
            DefaultLogger.info(String.format("Name Node Agent environment initialized. [source=%s]",
                    NameNodeEnv.get(name).source()));
            assertNotNull(NameNodeEnv.get(name).hdfsConnection());
            assertNotNull(NameNodeEnv.get(name).stateManager().connection());
            Thread[] threads = new Thread[5];
            for (int ii = 0; ii < 5; ii++) {
                threads[ii] = new Thread(new Locker(name));
                threads[ii].start();
            }
            for (int ii = 0; ii < 5; ii++) {
                threads[ii].join();
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }

    private static class Locker implements Runnable {
        private final String name;

        public Locker(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(500);
                try (DistributedLock gl = NameNodeEnv.get(name).globalLock()) {
                    try (DistributedLock lock = NameNodeEnv.get(name).createLock("LOCK_REPLICATION")) {
                        for (int ii = 0; ii < 5; ii++) {
                            gl.lock();
                            try {
                                lock.lock();
                                try {
                                    lock.lock(); // Checking re-entrance
                                    DefaultLogger.info(String.format("LOCK COUNTER=%d", lockCounter++));
                                } finally {
                                    lock.unlock();
                                }
                            } finally {
                                gl.unlock();
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                DefaultLogger.stacktrace(t);
                fail(t);
            }
        }
    }
}