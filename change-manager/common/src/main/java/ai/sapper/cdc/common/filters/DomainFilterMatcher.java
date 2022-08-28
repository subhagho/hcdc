package ai.sapper.cdc.common.filters;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
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
        private Filter filter;

        public boolean matches(@NonNull String value) {
            Matcher m = pattern.matcher(value);
            return m.matches();
        }
    }

    private final String domain;
    private final DomainFilters filters;
    private final List<PathFilter> patterns = new ArrayList<>();
    private Pattern ignoreRegex;

    public DomainFilterMatcher(@NonNull String domain, @NonNull DomainFilters filters) {
        this.domain = domain;
        this.filters = filters;
        List<Filter> fs = filters.get();
        if (fs != null && !fs.isEmpty()) {
            for (Filter f : fs) {
                String path = f.getPath();
                path = path.trim();
                if (path.endsWith("/")) {
                    path = path.substring(0, path.length() - 2);
                }
                PathFilter pf = new PathFilter();
                pf.filter = f;
                pf.path = path;
                pf.pattern = Pattern.compile(f.getRegex());
                patterns.add(pf);
            }
        }
    }

    public DomainFilterMatcher withIgnoreRegex(@NonNull Pattern regex) {
        ignoreRegex = regex;
        return this;
    }

    public PathFilter find(@NonNull Filter filter) {
        for (PathFilter pf : patterns) {
            if (pf.filter.equals(filter)) {
                return pf;
            }
        }
        return null;
    }

    public PathFilter matches(@NonNull String source) {
        source = PathUtils.formatPath(source.trim());
        for (PathFilter pf : patterns) {
            if (source.startsWith(pf.path)) {
                String part = source.replace(pf.path, "");
                if (part.startsWith("/")) {
                    part = part.substring(1);
                }
                DefaultLogger.LOGGER.debug(String.format("[dir=%s][part=%s]", pf.path, part));
                if (pf.matches(part)) {
                    if (ignoreRegex != null) {
                        Matcher ignore = ignoreRegex.matcher(source);
                        if (!ignore.matches())
                            return pf;
                    } else {
                        return pf;
                    }
                } else {
                    DefaultLogger.LOGGER.debug(String.format("Match failed: [dir=%s][part=%s]", pf.path, part));
                }
            }
        }
        return null;
    }

    public PathFilter add(@NonNull String entity,
                          @NonNull String path,
                          @NonNull String regex,
                          String group) {
        Filter df = filters.add(entity, path, regex, group);

        PathFilter pf = new PathFilter();
        pf.path = path;
        pf.filter = df;
        pf.pattern = Pattern.compile(pf.filter.getRegex());
        patterns.add(pf);

        return pf;
    }

    public DomainFilter updateGroup(@NonNull String entity,
                                    @NonNull String group) {
        return filters.updateGroup(entity, group);
    }

    public DomainFilter remove(@NonNull String entity) {
        return filters.remove(entity);
    }

    public List<Filter> remove(@NonNull String entity,
                               @NonNull String path) {
        return filters.remove(entity, path);
    }

    public Filter remove(@NonNull String entity,
                         @NonNull String path,
                         @NonNull String regex) {
        return filters.remove(entity, path, regex);
    }
}
