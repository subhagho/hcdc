package ai.sapper.cdc.core.filters;

import lombok.NonNull;

public interface FilterAddCallback {
    void process(@NonNull DomainFilterMatcher matcher, DomainFilterMatcher.PathFilter filter, @NonNull String path);

    void onStart(@NonNull DomainFilterMatcher matcher);
}
