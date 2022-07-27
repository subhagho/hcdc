package ai.sapper.hcdc.common.model.filters;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class DomainFilterMatcher {
    private static final String IGNORE_REGEX = ".*\\._COPYING_";
    private static final Pattern IGNORE_PATTERN = Pattern.compile(IGNORE_REGEX);

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class PathFilter {
        private String path;
        private Pattern pattern;
        private DomainFilter filter;

        public boolean matches(@NonNull String value) {
            Matcher m = pattern.matcher(value);
            return m.matches();
        }
    }

    private final String domain;
    private final DomainFilters filters;
    private final List<PathFilter> patterns;

    public DomainFilterMatcher(@NonNull String domain, @NonNull DomainFilters filters) {
        this.domain = domain;
        this.filters = filters;
        patterns = new ArrayList<>(filters.getFilters().size());
        for (String path : filters.keySet()) {
            PathFilter pf = new PathFilter();
            pf.path = path;
            pf.filter = filters.get(path);
            pf.pattern = Pattern.compile(pf.filter.getRegex());

            patterns.add(pf);
        }
    }

    public PathFilter find(@NonNull DomainFilter filter) {
        for (PathFilter pf : patterns) {
            if (pf.filter.equals(filter)) {
                return pf;
            }
        }
        return null;
    }

    public PathFilter matches(@NonNull String source) {
        source = source.trim();
        for (PathFilter pf : patterns) {
            if (source.startsWith(pf.path)) {
                String part = source.replace(pf.path, "");
                if (part.startsWith("/")) {
                    part = part.substring(1);
                }
                if (pf.matches(part)) {
                    Matcher ignore = IGNORE_PATTERN.matcher(source);
                    if (!ignore.matches())
                        return pf;
                }
            }
        }
        return null;
    }

    public PathFilter add(@NonNull String entity, @NonNull String path, @NonNull String regex) {
        DomainFilter df = filters.add(entity, path, regex);

        PathFilter pf = new PathFilter();
        pf.path = path;
        pf.filter = df;
        pf.pattern = Pattern.compile(pf.filter.getRegex());
        patterns.add(pf);

        return pf;
    }
}
