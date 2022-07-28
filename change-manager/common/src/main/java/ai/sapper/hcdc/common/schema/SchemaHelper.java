package ai.sapper.hcdc.common.schema;

import ai.sapper.hcdc.common.utils.ReflectionUtils;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaHelper {
    public enum EDataType {
        NULL, String, Number, Object, Array, Boolean;

        public static EDataType parse(String value) {
            for (EDataType dt : EDataType.values()) {
                if (dt.name().compareToIgnoreCase(value) == 0) {
                    return dt;
                }
            }
            return null;
        }
    }

    public enum ENumberType {
        Integer, Long, Float, Double, Short
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static abstract class Field {
        private final String name;
        private final EDataType type;
        private boolean nullable = true;

        private Field(@NonNull String name, @NonNull EDataType type) {
            this.name = name;
            this.type = type;
        }

        public abstract boolean check(String value);

        public String jsonSchema() {
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("\"%s\":", name));
            builder.append(String.format("{ \"type\": \"%s\" }", type.name().toLowerCase()));
            return null;
        }

        public static boolean isNullValue(String value) {
            return Strings.isNullOrEmpty(value)
                    || value.compareToIgnoreCase("null") == 0;
        }

        public static Field parseField(String name, Object value) {
            if (value == null) {
                return new NullField(name);
            }
            if (value instanceof String) {
                String sv = (String) value;
                if (isNullValue(sv)) {
                    return new NullField(name);
                }
                if (BooleanField.matches(sv)) {
                    return new BooleanField(name);
                } else if (NumberField.matches(sv)) {
                    return new NumberField(name);
                } else {
                    return new StringField(name);
                }
            } else if (ReflectionUtils.isNumericType(value.getClass())) {
                NumberField f = new NumberField(name);
                Class<?> type = value.getClass();
                if (type.equals(Short.class) || type.equals(short.class)) {
                    f.numberType = ENumberType.Short;
                } else if (type.equals(Integer.class) || type.equals(int.class)) {
                    f.numberType = ENumberType.Integer;
                } else if (type.equals(Long.class) || type.equals(long.class)) {
                    f.numberType = ENumberType.Long;
                } else if (type.equals(Float.class) || type.equals(float.class)) {
                    f.numberType = ENumberType.Float;
                } else if (type.equals(Double.class) || type.equals(double.class)) {
                    f.numberType = ENumberType.Double;
                } else {
                    f.numberType = ENumberType.Long;
                }
            } else if (value instanceof List) {
                List<?> values = (List<?>) value;
                return ArrayField.parse(name, values);
            } else if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) value;
                return ObjectField.parse(name, (Map<String, Object>) map);
            }
            return null;
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class NullField extends Field {

        private NullField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "null" : name), EDataType.NULL);
        }

        /**
         * @param value
         * @return
         */
        @Override
        public boolean check(String value) {
            return isNullValue(value);
        }

        public static boolean matches(String value) {
            return isNullValue(value);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class StringField extends Field {
        private static final String REGEX = "\"(\\.+)\"";
        private static final Pattern PATTERN = Pattern.compile(REGEX);

        public StringField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "string" : name), EDataType.String);
        }

        public static boolean matches(String value) {
            Matcher m = PATTERN.matcher(value);
            return m.matches();
        }

        /**
         * @param value
         * @return
         */
        @Override
        public boolean check(String value) {
            if (isNullValue(value)) {
                nullable(true);
                return true;
            }
            Matcher m = PATTERN.matcher(value);
            return m.matches();
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class NumberField extends Field {
        private static final String REGEX = "(^\\d+\\)(.?\\d*)$)";
        private static final Pattern PATTERN = Pattern.compile(REGEX);

        private ENumberType numberType;

        public NumberField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "number" : name), EDataType.Number);
        }

        public static boolean matches(String value) {
            Matcher m = PATTERN.matcher(value);
            return m.matches();
        }

        /**
         * @param value
         * @return
         */
        @Override
        public boolean check(String value) {
            if (isNullValue(value)) {
                nullable(true);
                return true;
            }
            Matcher m = PATTERN.matcher(value);
            if (m.matches()) {
                if (m.groupCount() > 1) {
                    if (m.group(2) != null) {
                        numberType = ENumberType.Double;
                    } else {
                        return false;
                    }
                }
                numberType = ENumberType.Long;
                return true;
            }
            return false;
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class BooleanField extends Field {

        private boolean isDecimal = false;

        public BooleanField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "boolean" : name), EDataType.Boolean);
        }

        public static boolean matches(String value) {
            Boolean bool = Boolean.parseBoolean(value);
            if (!bool) {
                if (!Strings.isNullOrEmpty(value)) {
                    value = value.trim();
                    value = value.replaceAll("\"", "");
                    if ("yes".compareToIgnoreCase(value) == 0
                            || "no".compareToIgnoreCase(value) == 0) {
                        bool = true;
                    }
                }
            }
            return bool;
        }

        /**
         * @param value
         * @return
         */
        @Override
        public boolean check(String value) {
            if (isNullValue(value)) {
                nullable(true);
                return true;
            }
            return matches(value);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ObjectField extends Field {
        private static final String REGEX = "^\\{\\s*(.*\\r*\\n*\\s*)}$";
        private static final Pattern PATTERN = Pattern.compile(REGEX);
        private List<Field> fields;

        public ObjectField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "object" : name), EDataType.Object);
        }

        public static boolean matches(String value) {
            value = value.replaceAll("[\\n\t\\r]", "");
            Matcher m = PATTERN.matcher(value);
            return m.matches();
        }

        /**
         * @param value
         * @return
         */
        @Override
        public boolean check(String value) {
            if (isNullValue(value)) {
                nullable(true);
                return true;
            }
            value = value.replaceAll("[\\n\t\\r]", "");
            Matcher m = PATTERN.matcher(value);
            return m.matches();
        }

        public boolean hasFields() {
            return (fields != null && !fields.isEmpty());
        }

        public void addField(@NonNull Field field) {
            if (fields == null) {
                fields = new ArrayList<>();
            }
            fields.add(field);
        }

        public static ObjectField parse(String name, @NonNull Map<String, Object> values) {
            if (Strings.isNullOrEmpty(name)) {
                name = "object";
            }
            ObjectField of = new ObjectField(name);
            for (String key : values.keySet()) {
                Field f = parseField(key, values.get(name));
                if (f != null) {
                    of.addField(f);
                }
            }
            return of;
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ArrayField extends Field {
        private static final String REGEX = "^\\[\\s*(.*\\r*\\n*\\s*)]$";
        private static final Pattern PATTERN = Pattern.compile(REGEX);
        private Field innerType;

        public ArrayField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "array" : name), EDataType.Array);
        }

        public static boolean matches(String value) {
            Matcher m = PATTERN.matcher(value);
            return m.matches();
        }

        /**
         * @param value
         * @return
         */
        @Override
        public boolean check(String value) {
            if (isNullValue(value)) {
                nullable(true);
                return true;
            }
            value = value.replaceAll("[\\n\t\\r]", "");
            Matcher m = PATTERN.matcher(value);
            return m.matches();
        }

        public static ArrayField parse(String name, @NonNull List<?> values) {
            ArrayField array = new ArrayField(name);
            if (values.isEmpty()) {
                array.innerType = new NullField("");
            } else {
                Field type = null;
                for (Object value : values) {
                    type = Field.parseField("", value);
                    if (type != null && !(type instanceof NullField)) {
                        break;
                    }
                }
                array.innerType = type;
            }
            return array;
        }
    }
}
