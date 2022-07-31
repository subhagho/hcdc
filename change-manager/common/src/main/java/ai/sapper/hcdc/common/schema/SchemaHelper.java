package ai.sapper.hcdc.common.schema;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.common.utils.ReflectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaHelper {
    public enum EDataType {
        NULL, String, Number, Object, Array, Boolean, Enum, Map;

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
        private boolean nullable = false;

        private Field(@NonNull String name, @NonNull EDataType type) {
            this.name = name;
            this.type = type;
        }

        public abstract boolean check(String value);

        public String avroSchema() {
            return String.format("{\"name\": \"%s\", \"type\": \"%s\"}", name(), avroType());
        }

        public abstract String avroType();

        public abstract boolean matches(@NonNull Field target);

        public static boolean isNullValue(String value) {
            return Strings.isNullOrEmpty(value)
                    || value.compareToIgnoreCase("null") == 0;
        }

        public static Field parseField(String name, Class<?> type) {
            if (type.equals(String.class)) {
                return new StringField(name);
            } else if (ReflectionUtils.isNumericType(type)) {
                NumberField f = new NumberField(name);
                f.fromJavaType(type);
                return f;
            } else if (type.equals(Boolean.class)
                    || type.equals(boolean.class)) {
                return new BooleanField(name);
            }
            return null;
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
                f.fromJavaType(value.getClass());
                return f;
            } else if (value.getClass().equals(Boolean.class)
                    || value.getClass().equals(boolean.class)) {
                return new BooleanField(name);
            } else if (value instanceof List) {
                List<?> values = (List<?>) value;
                return ArrayField.parse(name, values);
            } else if (value instanceof Map) {
                MapField mf = Field.isMapObject(name, (Map<String, ?>) value);
                if (mf != null) return mf;
                return ObjectField.parse(name, (Map<String, Object>) value);
            }
            return null;
        }

        private static MapField isMapObject(String name, Map<String, ?> values) {
            Class<?> vtype = null;
            for (Object value : values.values()) {
                if (value != null) {
                    Class<?> vt = value.getClass();
                    if (!ReflectionUtils.isPrimitiveTypeOrString(vt)) {
                        return null;
                    }
                    if (vtype == null) {
                        vtype = vt;
                    } else if (vt.equals(Strings.class) && !vtype.equals(String.class)) {
                        return null;
                    } else if (ReflectionUtils.isNumericType(vt) && !ReflectionUtils.isNumericType(vtype)) {
                        return null;
                    } else if (!vt.equals(vtype)) {
                        if (vtype.equals(Double.class)) continue;
                        else if (vtype.equals(Float.class)) {
                            if (vt.equals(Double.class)) {
                                vtype = vt;
                            }
                        } else if (vtype.equals(Long.class)) {
                            if (vt.equals(Float.class)
                                    || vt.equals(Double.class)) {
                                vtype = vt;
                            }
                        } else if (vtype.equals(Integer.class)) {
                            if (vt.equals(Long.class)
                                    || vt.equals(Float.class)
                                    || vt.equals(Double.class)) {
                                vtype = vt;
                            }
                        } else if (vtype.equals(Short.class)) {
                            if (vt.equals(Long.class)
                                    || vt.equals(Float.class)
                                    || vt.equals(Double.class)
                                    || vt.equals(Integer.class)) {
                                vtype = vt;
                            }
                        }
                    }
                }
            }
            if (vtype != null) {
                Field inner = parseField("value", vtype);
                return new MapField(name).innerType(inner);
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

        /**
         * @return
         */
        @Override
        public String avroSchema() {
            return String.format("{\"name\": \"%s\", \"type\": \"%s\"}", name(), EDataType.String.name().toLowerCase());
        }

        /**
         * @return
         */
        @Override
        public String avroType() {
            return EDataType.String.name().toLowerCase();
        }

        /**
         * @param target
         * @return
         */
        @Override
        public boolean matches(@NonNull Field target) {
            if (target instanceof NullField) {
                return name().compareTo(target.name) == 0;
            }
            return false;
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

        /**
         * @return
         */
        @Override
        public String avroType() {
            return type().name().toLowerCase();
        }

        @Override
        public boolean matches(@NonNull Field target) {
            if (target instanceof StringField) {
                return name().compareTo(target.name) == 0;
            }
            return false;
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

        public NumberField fromJavaType(Class<?> type) {
            Preconditions.checkArgument(ReflectionUtils.isNumericType(type));
            if (type.equals(Short.class) || type.equals(short.class)) {
                numberType = ENumberType.Short;
            } else if (type.equals(Integer.class) || type.equals(int.class)) {
                numberType = ENumberType.Integer;
            } else if (type.equals(Long.class) || type.equals(long.class)) {
                numberType = ENumberType.Long;
            } else if (type.equals(Float.class) || type.equals(float.class)) {
                numberType = ENumberType.Float;
            } else if (type.equals(Double.class) || type.equals(double.class)) {
                numberType = ENumberType.Double;
            } else {
                numberType = ENumberType.Long;
            }
            return this;
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

        /**
         * @return
         */
        @Override
        public String avroType() {
            switch (numberType) {
                case Integer:
                case Short:
                    return "int";
                case Long:
                case Float:
                case Double:
                    return numberType.name().toLowerCase();
            }
            return ENumberType.Long.name().toLowerCase();
        }

        @Override
        public boolean matches(@NonNull Field target) {
            if (target instanceof NumberField && numberType == ((NumberField) target).numberType) {
                return name().compareTo(target.name) == 0;
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

        /**
         * @return
         */
        @Override
        public String avroType() {
            return type().name().toLowerCase();
        }

        @Override
        public boolean matches(@NonNull Field target) {
            if (target instanceof BooleanField) {
                return name().compareTo(target.name) == 0;
            }
            return false;
        }

    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ObjectField extends Field {
        private static final String REGEX = "^\\{\\s*(.*\\r*\\n*\\s*)}$";
        private static final Pattern PATTERN = Pattern.compile(REGEX);
        private List<Field> fields;
        private String namespace = "ai.sapper.hcdc";

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

        /**
         * @return
         */
        @Override
        public String avroType() {
            return type().name().toLowerCase();
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

        @Override
        public boolean matches(@NonNull Field target) {
            if (target instanceof ObjectField) {
                if (name().compareTo(target.name) == 0) {
                    if (fields != null && !fields.isEmpty()) {
                        for (Field field : fields) {
                            if (!target.matches(field)) return false;
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * @return
         */
        @Override
        public String avroSchema() {
            StringBuilder builder = new StringBuilder();
            builder.append(
                    String.format("{\n\"type\": \"record\",\n\"namespace\": \"%s\",\n\"name\": \"%s\",\n\"fields\": [\n",
                            namespace, name()));
            if (fields != null && !fields.isEmpty()) {
                boolean first = true;
                for (Field field : fields) {
                    if (first) first = false;
                    else {
                        builder.append(",\n");
                    }
                    if (field instanceof ObjectField
                            || field instanceof ArrayField
                            || field instanceof MapField) {
                        builder.append(String.format("{\"name\": \"%s\",\n\"type\": %s\n}",
                                field.name, field.avroSchema()));
                    } else
                        builder.append(field.avroSchema());
                }
            }
            builder.append("\n]\n}");
            return builder.toString();
        }

        private boolean hasField(Field field) {
            if (fields != null && !fields.isEmpty()) {
                for (Field f : fields) {
                    if (f.matches(field)) {
                        return true;
                    }
                }
            }
            return false;
        }

        public static ObjectField parse(String name, @NonNull Map<String, Object> values) {
            ObjectField of = new ObjectField(name);
            for (String key : values.keySet()) {
                Field f = parseField(key, values.get(key));
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

        /**
         * @return
         */
        @Override
        public String avroType() {
            return type().name().toLowerCase();
        }

        /**
         * @param target
         * @return
         */
        @Override
        public boolean matches(@NonNull Field target) {
            if (target instanceof ArrayField) {
                if (name().compareTo(target.name()) == 0) {
                    if (innerType != null && ((ArrayField) target).innerType != null) {
                        return innerType.matches(((ArrayField) target).innerType);
                    }
                }
            }
            return false;
        }

        /**
         * @return
         */
        @Override
        public String avroSchema() {
            return String.format("{ \"type\": \"%s\", \"items\": \"%s\" }", avroType(), innerType.avroType());
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

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class MapField extends Field {
        private Field innerType;

        private MapField(@NonNull String name) {
            super(name, EDataType.Map);
        }

        /**
         * @param value
         * @return
         */
        @Override
        public boolean check(String value) {
            return false;
        }

        /**
         * @return
         */
        @Override
        public String avroType() {
            return type().name().toLowerCase();
        }

        /**
         * @return
         */
        @Override
        public String avroSchema() {
            return String.format("{ \"type\": \"%s\", \"values\": \"%s\" }", avroType(), innerType.avroType());
        }

        /**
         * @param target
         * @return
         */
        @Override
        public boolean matches(@NonNull Field target) {
            if (target instanceof MapField) {
                if (name().compareTo(target.name()) == 0) {
                    if (innerType != null && ((MapField) target).innerType != null) {
                        return innerType.matches(((MapField) target).innerType);
                    }
                }
            }
            return false;
        }

        public static MapField parse(String name, @NonNull Map<?, ?> values) {
            MapField map = new MapField(name);
            if (values.isEmpty()) {
                map.innerType = new NullField("");
            } else {
                Field type = null;
                for (Object value : values.values()) {
                    type = Field.parseField("", value);
                    if (type != null && !(type instanceof NullField)) {
                        break;
                    }
                }
                map.innerType = type;
            }
            return map;
        }
    }

    public static class JsonToAvroSchema {
        public static Schema convert(@NonNull Map<String, Object> map,
                                     String namespace,
                                     @NonNull String name,
                                     @NonNull ObjectMapper mapper) throws Exception {
            Preconditions.checkArgument(!map.isEmpty());
            Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

            SchemaHelper.ObjectField field = SchemaHelper.ObjectField.parse(name, map);
            if (!Strings.isNullOrEmpty(namespace))
                field.namespace(namespace);
            String avroSchema = field.avroSchema();
            return new Schema.Parser().parse(avroSchema);
        }

        public static Schema convert(@NonNull String json,
                                     String namespace,
                                     @NonNull String name) throws Exception {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(json));
            Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(json, Map.class);

            SchemaHelper.ObjectField field = SchemaHelper.ObjectField.parse(name, map);
            if (!Strings.isNullOrEmpty(namespace))
                field.namespace(namespace);
            String avroSchema = field.avroSchema();
            return new Schema.Parser().parse(avroSchema);
        }
    }

    public static class POJOToAvroSchema {
        public static Schema convert(@NonNull Object data) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(data);
            Map<String, Object> map = mapper.readValue(json, Map.class);
            DefaultLogger.LOG.debug(String.format("\nJSON: [\n%s\n]", json));
            SchemaHelper.ObjectField field =
                    SchemaHelper.ObjectField.parse(data.getClass().getSimpleName(), map);
            field.namespace(data.getClass().getCanonicalName());
            String avroSchema = field.avroSchema();
            return new Schema.Parser().parse(avroSchema);
        }
    }
}
