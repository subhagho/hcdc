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

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DFSEditsFileFinder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DFSEditsFileFinderTest {
    private static final String SOURCE_DIR = "src/test/resources/edits";

    @Test
    void findEditsFiles() {
        try {
            List<DFSEditsFileFinder.EditsLogFile> paths = DFSEditsFileFinder.findEditsFiles(SOURCE_DIR, 7, 17);
            assertNotNull(paths);
            for (DFSEditsFileFinder.EditsLogFile path : paths) {
                DefaultLogger.info(String.format("Files in Range [%s]", path));
            }
            paths = DFSEditsFileFinder.findEditsFiles(SOURCE_DIR, -1, 17);
            assertNotNull(paths);
            for (DFSEditsFileFinder.EditsLogFile path : paths) {
                DefaultLogger.info(String.format("File till TX [%s]", path));
            }
            paths = DFSEditsFileFinder.findEditsFiles(SOURCE_DIR, 17, -1);
            assertNotNull(paths);
            for (DFSEditsFileFinder.EditsLogFile path : paths) {
                DefaultLogger.info(String.format("Files with Start TX [%s]", path));
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }

    @Test
    void findSeenTxID() {
        try {
            long txid = DFSEditsFileFinder.findSeenTxID(SOURCE_DIR);
            assertTrue(txid >= 0);
            DefaultLogger.info(String.format("Found TXID = %d", txid));
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}