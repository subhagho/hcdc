package ai.sapper.hcdc.core.filters;

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

    public void add(@NonNull String path, @NonNull String regex) {
        DomainFilter filter = get(path);
        if (filter == null) {
            filter = filters.put(path, new DomainFilter(path, regex));
        } else {
            filter.setRegex(regex);
            filter.setUpdatedTime(System.currentTimeMillis());
        }
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
