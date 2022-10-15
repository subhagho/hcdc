package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.connections.Connection;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public abstract class ConnectionSettings {
    public static final String CONFIG_CLASS = "@class";

    @Setting(name = "type")
    private EConnectionType type;
    @Setting(name = "source")
    private ESettingsSource source;
    @Setting(name = "name")
    private String name;
    @Setting(name = "parameters", required = false, parser = MapSettingsParser.class)
    private Map<String, String> parameters;
    @Setting(name = "class")
    private Class<? extends Connection> connectionClass;

    public ConnectionSettings() {
        source = ESettingsSource.File;
    }

    public ConnectionSettings(@NonNull ConnectionSettings settings) {
        type = settings.type;
        source = settings.source;
        name = settings.name;
        if (settings.parameters != null) {
            parameters = new HashMap<>(settings.parameters);
        }
        connectionClass = settings.connectionClass;
    }

    public abstract void validate() throws Exception;

    public static ConnectionSettings read(@NonNull Map<String, String> settings) throws Exception {
        String cls = settings.get(CONFIG_CLASS);
        ConfigReader.checkStringValue(cls, ConnectionSettings.class, CONFIG_CLASS);
        Class<? extends ConnectionSettings> t = (Class<? extends ConnectionSettings>) Class.forName(cls);
        ConnectionSettings value = t.getDeclaredConstructor().newInstance();
        Field[] fields = ReflectionUtils.getAllFields(t);
        if (fields != null && fields.length > 0) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Setting.class)) {
                    Setting s = field.getAnnotation(Setting.class);
                    String v = settings.get(s.name());
                    if (Strings.isNullOrEmpty(v)) {
                        if (s.required()) {
                            throw new Exception(String.format("Missing required parameter: [name=%s]", s.name()));
                        }
                    } else {
                        if (s.parser() != null && !s.parser().equals(SettingParser.NullSettingParser.class)) {
                            SettingParser<?> parser = s.parser().getDeclaredConstructor().newInstance();
                            Object o = parser.parse(v);
                            if (o == null && s.required()) {
                                throw new Exception(String.format("NULL/Empty required parameter: [name=%s]", s.name()));
                            }
                            ReflectionUtils.setObjectValue(value, field, o);
                        } else {
                            if (ReflectionUtils.isPrimitiveTypeOrString(field))
                                ReflectionUtils.setPrimitiveValue(v, value, field);
                            else if (field.getType().isEnum()) {
                                ReflectionUtils.setValueFromString(v, value, field);
                            }
                        }
                    }
                }
            }
        }
        return value;
    }

    public static Map<String, String> serialize(@NonNull ConnectionSettings settings) throws Exception {
        Map<String, String> values = new HashMap<>();
        Field[] fields = ReflectionUtils.getAllFields(settings.getClass());
        if (fields != null && fields.length > 0) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Setting.class)) {
                    Setting s = field.getAnnotation(Setting.class);
                    Object o = ReflectionUtils.getFieldValue(settings, field);
                    if (o != null) {
                        if (s.parser() != null && !s.parser().equals(SettingParser.NullSettingParser.class)) {
                            SettingParser<?> parser = s.parser().getDeclaredConstructor().newInstance();
                            String v = parser.serialize(o);
                            values.put(s.name(), v);
                        } else if (ReflectionUtils.isPrimitiveTypeOrString(field)) {
                            if (field.getType().equals(Class.class)) {
                                Class<?> cls = (Class<?>) o;
                                values.put(s.name(), cls.getCanonicalName());
                            } else
                                values.put(s.name(), String.valueOf(o));
                        } else if (field.getType().isEnum()) {
                            Enum<?> e = (Enum<?>) o;
                            values.put(s.name(), e.name());
                        }
                    } else if (s.required()) {
                        throw new Exception(String.format("Required field not set. [field=%s]", field.getName()));
                    }
                }
            }
        }
        values.put(CONFIG_CLASS, settings.getClass().getCanonicalName());
        return values;
    }
}
