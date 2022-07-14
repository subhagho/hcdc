package ai.sapper.hcdc.core;

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

    private final DomainFilter filter;
    private final List<PathFilter> patterns;

    public DomainFilterMatcher(@NonNull DomainFilter filter) {
        this.filter = filter;
        patterns = new ArrayList<>(filter.getFilters().size());
        for (String path : filter.getFilters().keySet()) {
            PathFilter pf = new PathFilter();
            pf.path = path;
            pf.pattern = Pattern.compile(filter.getFilters().get(path));
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

    public DomainFilter add(@NonNull String path, @NonNull String regex) {
        filter.add(path, regex);

        PathFilter pf = new PathFilter();
        pf.path = path;
        pf.pattern = Pattern.compile(filter.getFilters().get(path));
        patterns.add(pf);

        return filter;
    }
}
