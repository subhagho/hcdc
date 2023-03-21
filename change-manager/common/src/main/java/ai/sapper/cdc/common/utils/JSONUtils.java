package ai.sapper.cdc.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.nio.charset.Charset;
import java.util.Map;

public class JSONUtils {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static ObjectMapper mapper() {
        return mapper;
    }

    public static byte[] asBytes(@NonNull Object obj, @NonNull Class<?> type) throws JsonProcessingException {
        String json = mapper.writeValueAsString(obj);
        if (!Strings.isNullOrEmpty(json)) {
            return json.getBytes(Charset.defaultCharset());
        }
        return null;
    }

    public static String asString(@NonNull Object obj, @NonNull Class<?> type) throws JsonProcessingException {
        return mapper.writeValueAsString(obj);
    }

    public static <T> T read(byte[] data, Class<? extends T> type) throws JsonProcessingException {
        String json = new String(data, Charset.defaultCharset());
        return mapper.readValue(json, type);
    }

    public static <T> T read(String data, Class<? extends T> type) throws JsonProcessingException {
        return mapper.readValue(data, type);
    }

    public static boolean isJson(@NonNull String value) {
        if (!Strings.isNullOrEmpty(value)) {
            try {
                mapper.readValue(value, Map.class);
                return true;
            } catch (Exception ex) {
                return false;
            }
        }
        return false;
    }
}
