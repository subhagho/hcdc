package ai.sapper.hcdc.core.filters;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class DomainFilterMatcher {
    private static class PathFilter {
        private String path;
        private Pattern pattern;
    }

    private final DomainFilters filters;
    private final List<PathFilter> patterns;

    public DomainFilterMatcher(@NonNull DomainFilters filters) {
        this.filters = filters;
        patterns = new ArrayList<>(filters.getFilters().size());
        for (String path : filters.keySet()) {
            PathFilter pf = new PathFilter();
            pf.path = path;
            pf.pattern = Pattern.compile(filters.get(path).getRegex());

            patterns.add(pf);
        }
    }

    public boolean matches(@NonNull String source) {
        source = source.trim();
        for (PathFilter pf : patterns) {
            if (source.startsWith(pf.path)) {
                String part = source.replace(pf.path, "");
                if (part.startsWith("/")) {
                    part = part.substring(1);
                }
                Matcher m = pf.pattern.matcher(part);
                if (m.matches()) return true;
            }
        }
        return false;
    }

    public DomainFilters add(@NonNull String path, @NonNull String regex) {
        filters.add(path, regex);

        PathFilter pf = new PathFilter();
        pf.path = path;
        pf.pattern = Pattern.compile(filters.get(path).getRegex());
        patterns.add(pf);

        return filters;
    }
}
