package ai.sapper.cdc.common.model;

import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

public class Context {
    public final Map<String, Object> params = new HashMap<>();

    public Context() {
    }

    public Context(@NonNull Context source) {
        params.putAll(source.params);
    }

    public Context(@NonNull Map<String, Object> params) {
        this.params.putAll(params);
    }

    public Context put(@NonNull String key, Object value) {
        params.put(key, value);
        return this;
    }

    public Object get(@NonNull String key) {
        return params.get(key);
    }

    public boolean containsKey(@NonNull String key) {
        return params.containsKey(key);
    }

    public boolean remove(@NonNull String key) {
        return (params.remove(key) != null);
    }

    public void clear() {
        params.clear();
    }

    public boolean isEmpty() {
        return params.isEmpty();
    }
}
