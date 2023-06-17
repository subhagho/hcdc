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

import ai.sapper.cdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CSVDataReaderTest {
    private static final String __INPUT_FILE = "src/test/resources/movies.csv";

    @Test
    void read() {
        try {
            CSVDataReader reader = new CSVDataReader(__INPUT_FILE, Character.MIN_VALUE);
            List<List<String>> records = reader.read();
            assertNotNull(records);
            assertTrue(records.size() > 0);
            for (List<String> record : records) {
                DefaultLogger.info(record.toString());
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}