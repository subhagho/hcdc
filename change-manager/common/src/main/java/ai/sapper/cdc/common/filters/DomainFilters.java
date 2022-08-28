package ai.sapper.cdc.common.filters;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.*;

@Getter
@Setter
public class DomainFilters {
    private String domain;
    private final Map<String, DomainFilter> filters = new HashMap<>();

    public synchronized Filter add(@NonNull String entity,
                                   @NonNull String path,
                                   @NonNull String regex,
                                   String group) {
        DomainFilter filter = filters.get(entity);
        if (filter == null) {
            filter = new DomainFilter(domain, entity).withGroup(group);
            filters.put(entity, filter);
        }
        return filter.add(path, regex);
    }

    public DomainFilter updateGroup(@NonNull String entity, @NonNull String group) {
        DomainFilter filter = filters.get(entity);
        if (filter != null) {
            return filter.withGroup(group);
        }
        return null;
    }

    public DomainFilter getDomainFilter(@NonNull String entity) {
        if (filters.containsKey(entity)) {
            return filters.get(entity);
        }
        return null;
    }

    public List<Filter> get(@NonNull String path) {
        if (!filters.isEmpty()) {
            List<Filter> fs = new ArrayList<>();
            for (String key : filters.keySet()) {
                DomainFilter df = filters.get(key);
                List<Filter> dfs = df.find(path);
                if (dfs != null && !dfs.isEmpty()) {
                    fs.addAll(dfs);
                }
            }
            if (!fs.isEmpty()) return fs;
        }
        return null;
    }

    public List<Filter> get() {
        if (!filters.isEmpty()) {
            List<Filter> fs = new ArrayList<>();
            for (String key : filters.keySet()) {
                DomainFilter df = filters.get(key);
                List<Filter> dfs = df.getFilters();
                if (dfs != null && !dfs.isEmpty()) {
                    fs.addAll(dfs);
                }
            }
            if (!fs.isEmpty()) return fs;
        }
        return null;
    }

    public DomainFilter remove(@NonNull String entity) {
        if (filters.containsKey(entity)) {
            DomainFilter filter = filters.get(entity);
            filters.remove(entity);
            return filter;
        }
        return null;
    }

    public List<Filter> remove(@NonNull String entity,
                               @NonNull String path) {
        if (filters.containsKey(entity)) {
            DomainFilter df = filters.get(entity);
            return df.remove(path);
        }
        return null;
    }

    public Filter remove(@NonNull String entity,
                         @NonNull String path,
                         @NonNull String regex) {
        if (filters.containsKey(entity)) {
            DomainFilter df = filters.get(entity);
            return df.remove(path, regex);
        }
        return null;
    }

    public Set<String> keySet() {
        return filters.keySet();
    }

}
