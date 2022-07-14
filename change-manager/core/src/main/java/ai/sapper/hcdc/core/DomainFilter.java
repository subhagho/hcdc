package ai.sapper.hcdc.core;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class DomainFilter {
    private String name;
    private Map<String, String> filters = new HashMap<>();

    public void add(@NonNull String path, @NonNull String regex) {
        filters.put(path, regex);
    }
}
