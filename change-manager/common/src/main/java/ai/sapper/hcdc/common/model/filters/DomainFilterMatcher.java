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

    private final DomainFilters filters;
    private final List<PathFilter> patterns;

    public DomainFilterMatcher(@NonNull DomainFilters filters) {
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

    public boolean matches(@NonNull String source) {
        source = source.trim();
        for (PathFilter pf : patterns) {
            if (source.startsWith(pf.path)) {
                String part = source.replace(pf.path, "");
                if (part.startsWith("/")) {
                    part = part.substring(1);
                }
                if (pf.matches(part)) return true;
            }
        }
        return false;
    }

    public PathFilter add(@NonNull String path, @NonNull String regex) {
        DomainFilter df = filters.add(path, regex);

        PathFilter pf = new PathFilter();
        pf.path = path;
        pf.filter = df;
        pf.pattern = Pattern.compile(pf.filter.getRegex());
        patterns.add(pf);

        return pf;
    }
}
