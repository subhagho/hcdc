package ai.sapper.cdc.core.filters;

import ai.sapper.cdc.common.filters.DomainFilterMatcher;
import ai.sapper.cdc.common.filters.DomainFilters;
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
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}