package ai.sapper.cdc.core.connections.settngs;

import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

public class MapSettingsParser implements SettingParser<Map<String, String>> {
    @Override
    public Map<String, String> parse(@NonNull String value) throws Exception {
        if (!Strings.isNullOrEmpty(value)) {
            String[] parts = value.split(";");
            if (parts.length > 0) {
                Map<String, String> params = new HashMap<>();
                for (String part : parts) {
                    String[] kv = part.split("=");
                    if (kv.length == 2) {
                        String k = kv[0].trim();
                        String v = kv[1].trim();
                        params.put(k, v);
                    }
                }
                if (!params.isEmpty()) return params;
            }
        }
        return null;
    }

    @Override
    public String serialize(@NonNull Object source) throws Exception {
        Map<String, String> value = (Map<String, String>) source;
        if (value != null && !value.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            for (Map.Entry<String, String> entry : value.entrySet()) {
                if (first) first = false;
                else {
                    builder.append(";");
                }
                builder.append(entry.getKey()).append("=").append(entry.getValue());
            }
            return builder.toString();
        }
        return null;
    }
}
