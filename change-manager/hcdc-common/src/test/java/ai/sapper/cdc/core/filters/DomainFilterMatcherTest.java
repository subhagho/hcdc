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

package ai.sapper.cdc.core.filters;

import ai.sapper.cdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DomainFilterMatcherTest {

    @Test
    void matches() {
        try {
            DomainFilters filters = new DomainFilters();
            filters.setDomain("TEST-FILTERS");
            for (int ii = 0; ii < 5; ii++) {
                filters.add( String.format("ENTITY:%d", ii), "/a/b/c", "(.*)/d/(.*)\\.log", "default");
            }
            DomainFilterMatcher matcher = new DomainFilterMatcher(filters.getDomain(), filters);
            String mf = "/a/b/c/e/d/test.log";
            assertNotNull(matcher.matches(mf));
            mf = "/a/b/c/e/x/test.log";
            assertNull(matcher.matches(mf));
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}