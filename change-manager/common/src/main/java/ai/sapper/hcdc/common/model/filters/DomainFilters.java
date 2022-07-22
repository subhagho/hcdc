package ai.sapper.hcdc.common.model.filters;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
public class DomainFilters {
    private String name;
    private Map<String, DomainFilter> filters = new HashMap<>();

    public DomainFilter add(@NonNull String entity, @NonNull String path, @NonNull String regex) {
        DomainFilter filter = get(path);
        if (filter == null) {
            filter = new DomainFilter(entity, path, regex);
            filters.put(path, filter);
        } else {
            filter.setRegex(regex);
            filter.setUpdatedTime(System.currentTimeMillis());
        }
        return filter;
    }

    public DomainFilter get(@NonNull String path) {
        if (filters.containsKey(path)) {
            return filters.get(path);
        }
        return null;
    }

    public Set<String> keySet() {
        return filters.keySet();
    }

}
