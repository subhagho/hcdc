package ai.sapper.hcdc.core.filters;

import ai.sapper.hcdc.common.model.filters.DomainFilterMatcher;
import ai.sapper.hcdc.common.model.filters.DomainFilters;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DomainFilterMatcherTest {

    @Test
    void matches() {
        try {
            DomainFilters filters = new DomainFilters();
            filters.setName("TEST-FILTERS");
            for (int ii = 0; ii < 5; ii++) {
                filters.add("/a/b/c", "(.*)/d/(.*)\\.log");
            }
            DomainFilterMatcher matcher = new DomainFilterMatcher(filters);
            String mf = "/a/b/c/e/d/test.log";
            assertTrue(matcher.matches(mf));
            mf = "/a/b/c/e/x/test.log";
            assertFalse(matcher.matches(mf));
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}