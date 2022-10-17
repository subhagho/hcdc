package ai.sapper.cdc.common.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class Options {
    private Map<String, Object> options = new HashMap<>();

    public Options() {
    }

    public Options(@NonNull Options source) {
        if (!source.options.isEmpty())
            options.putAll(source.options);
    }

    public Options(@NonNull Map<String, Object> options) {
        if (!options.isEmpty())
            this.options.putAll(options);
    }

    public boolean containsKey(@NonNull String name) {
        return options.containsKey(name);
    }

    public Object get(@NonNull String name) {
        if (options.containsKey(name)) {
            return options.get(name);
        }
        return null;
    }

    public Object put(@NonNull String name, Object value) {
        return options.put(name, value);
    }

    public Object remove(@NonNull String name) {
        return options.remove(name);
    }

    public boolean isEmpty() {
        return options.isEmpty();
    }

    public int size() {
        return options.size();
    }

    public void clear() {
        options.clear();
    }

    public Optional<Boolean> getBoolean(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof Boolean) {
                return Optional.of((Boolean) o);
            }
        }
        return Optional.empty();
    }

    public Optional<Integer> getInt(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof Integer) {
                return Optional.of((Integer) o);
            }
        }
        return Optional.empty();
    }

    public Optional<Long> getLong(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof Long) {
                return Optional.of((Long) o);
            }
        }
        return Optional.empty();
    }

    public Optional<Double> getDouble(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof Double) {
                return Optional.of((Double) o);
            }
        }
        return Optional.empty();
    }

    public Optional<String> getString(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof String) {
                return Optional.of((String) o);
            }
        }
        return Optional.empty();
    }
}
