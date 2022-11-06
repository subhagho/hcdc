package ai.sapper.cdc.common.schema;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.HashMap;
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

    public static String checkFieldName(@NonNull String name) {
        StringBuilder builder = new StringBuilder();
        for (char c : name.toCharArray()) {
            if (!Character.isAlphabetic(c) && c != '_') {
                c = '_';
            }
            builder.append(c);
        }
        return builder.toString();
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static abstract class Field {
        private final String name;
        private final EDataType type;
        private boolean nullable = false;

        public Field(@NonNull String name,
                     @NonNull EDataType type) {
            this.name = checkFieldName(name);
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
                return f.fromJavaType(type);
            } else if (ReflectionUtils.isBoolean(type)) {
                return new BooleanField(name);
            }
            return null;
        }

        @SuppressWarnings("unchecked")
        public static Field parseField(@NonNull String name,
                                       Object value,
                                       boolean nested) throws Exception {
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
                    StringField sf = new StringField(name);
                    sf.length(((String) value).length());
                    return sf;
                }
            } else if (ReflectionUtils.isNumericType(value.getClass())) {
                NumberField f = new NumberField(name);
                return f.fromJavaType(value.getClass());
            } else if (ReflectionUtils.isBoolean(value.getClass())) {
                return new BooleanField(name);
            } else if (value instanceof List) {
                List<?> values = (List<?>) value;
                return ArrayField.parse(name, values, nested);
            } else if (value instanceof Map) {
                Map<String, ?> map = (Map<String, ?>) value;
                if (!map.isEmpty()) {
                    MapField mf = Field.isMapObject(name, (Map<String, ?>) value);
                    if (mf != null) return mf;
                    return ObjectField.parse(name, (Map<String, Object>) value, nested);
                }
            }
            return new NullField(name);
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
                        if (ReflectionUtils.isDouble(vtype)) continue;
                        else if (ReflectionUtils.isFloat(vtype)) {
                            if (ReflectionUtils.isDouble(vt)) {
                                vtype = vt;
                            }
                        } else if (ReflectionUtils.isLong(vtype)) {
                            if (ReflectionUtils.isFloat(vt)
                                    || ReflectionUtils.isDouble(vt)) {
                                vtype = vt;
                            }
                        } else if (ReflectionUtils.isInt(vtype)) {
                            if (ReflectionUtils.isLong(vt)
                                    || ReflectionUtils.isFloat(vt)
                                    || ReflectionUtils.isDouble(vt)) {
                                vtype = vt;
                            }
                        } else if (ReflectionUtils.isShort(vtype)) {
                            if (ReflectionUtils.isInt(vt)
                                    || ReflectionUtils.isLong(vt)
                                    || ReflectionUtils.isFloat(vt)
                                    || ReflectionUtils.isDouble(vt)) {
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

        public NullField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "null" : name), EDataType.NULL);
        }

        public NullField(@NonNull NullField field) {
            super((Strings.isNullOrEmpty(field.name()) ? "null" : field.name()), EDataType.NULL);
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
        private int length = 0;

        public StringField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "string" : name), EDataType.String);
        }

        public StringField(@NonNull StringField field) {
            super((Strings.isNullOrEmpty(field.name()) ? "string" : field.name()), EDataType.String);
            nullable(field.nullable());
            length = field.length;
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

        public NumberField(@NonNull NumberField field) {
            super((Strings.isNullOrEmpty(field.name()) ? "number" : field.name()), EDataType.Number);
            nullable(field.nullable());
            numberType = field.numberType;
        }

        public static boolean matches(String value) {
            Matcher m = PATTERN.matcher(value);
            return m.matches();
        }

        public NumberField fromJavaType(Class<?> type) {
            Preconditions.checkArgument(ReflectionUtils.isNumericType(type));
            if (ReflectionUtils.isShort(type)) {
                numberType = ENumberType.Short;
            } else if (ReflectionUtils.isInt(type)) {
                numberType = ENumberType.Integer;
            } else if (ReflectionUtils.isLong(type)) {
                numberType = ENumberType.Long;
            } else if (ReflectionUtils.isFloat(type)) {
                numberType = ENumberType.Float;
            } else if (ReflectionUtils.isDouble(type)) {
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

        public BooleanField(@NonNull BooleanField field) {
            super((Strings.isNullOrEmpty(field.name()) ? "boolean" : field.name()), EDataType.Boolean);
            nullable(field.nullable());
            isDecimal = field.isDecimal;
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
        private Map<String, Field> fields;
        private String namespace = "ai.sapper.cdc";

        public ObjectField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "object" : name), EDataType.Object);
        }

        public ObjectField(@NonNull ObjectField field) {
            super((Strings.isNullOrEmpty(field.name()) ? "object" : field.name()), EDataType.Object);
            nullable(field.nullable());
            namespace = field.namespace;
            if (field.fields != null) {
                fields = new HashMap<>(field.fields);
            }
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
                fields = new HashMap<>();
            }
            fields.put(field.name, field);
        }

        @Override
        public boolean matches(@NonNull Field target) {
            if (target instanceof ObjectField) {
                if (fields != null && !fields.isEmpty()) {
                    for (String name : fields.keySet()) {
                        Field field = fields.get(name);
                        if (!((ObjectField) target).hasField(field)) return false;
                    }
                    return true;
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
                for (String name : fields.keySet()) {
                    Field field = fields.get(name);
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
            if (fields != null && fields.containsKey(field.name)) {
                Field f = fields.get(field.name);
                return field.equals(f);
            }
            return false;
        }

        public static ObjectField parse(@NonNull String name,
                                        @NonNull Map<String, Object> values,
                                        boolean nested) throws Exception {
            ObjectField of = new ObjectField(name);
            for (String key : values.keySet()) {
                Field f = parseField(key, values.get(key), nested);
                if (f != null) {
                    of.addField(f);
                }
            }
            if (of.fields == null || of.fields.isEmpty()) {
                throw new Exception(String.format("Invalid Object type. [name=%s][values=%s]", name, values));
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

        public ArrayField(@NonNull ArrayField field) {
            super((Strings.isNullOrEmpty(field.name()) ? "array" : field.name()), EDataType.Array);
            nullable(field.nullable());
            innerType = field.innerType;
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
                if (innerType != null && ((ArrayField) target).innerType != null) {
                    return innerType.matches(((ArrayField) target).innerType);
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

        public static ArrayField parse(@NonNull String name,
                                       @NonNull List<?> values,
                                       boolean nested) throws Exception {
            ArrayField array = new ArrayField(name);
            if (values.isEmpty()) {
                array.innerType = new NullField("");
            } else {
                Field type = null;
                for (Object value : values) {
                    Field f = Field.parseField("inner", value, nested);
                    if (f instanceof NullField) {
                        continue;
                    }
                    if (type == null) {
                        type = f;
                    } else {
                        if (!type.equals(f)) {
                            if (ReflectionUtils.isPrimitiveTypeOrString(value.getClass())) {
                                type = new StringField("inner");
                            } else {
                                type = new JsonField("inner");
                                break;
                            }
                        }
                    }
                }
                if (type == null) {
                    type = new NullField("inner");
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

        public MapField(@NonNull String name) {
            super((Strings.isNullOrEmpty(name) ? "map" : name), EDataType.Map);
        }

        public MapField(@NonNull MapField field) {
            super((Strings.isNullOrEmpty(field.name()) ? "map" : field.name()), EDataType.Map);
            nullable(field.nullable());
            innerType = field.innerType;
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
                if (innerType != null && ((MapField) target).innerType != null) {
                    return innerType.matches(((MapField) target).innerType);
                }
            }
            return false;
        }

        public static MapField parse(@NonNull String name,
                                     @NonNull Map<?, ?> values,
                                     boolean nested) throws Exception {
            MapField map = new MapField(name);
            if (values.isEmpty()) {
                map.innerType = new NullField("");
            } else {
                Field type = null;
                for (Object key : values.keySet()) {
                    Object value = values.get(key);
                    Field f = Field.parseField("value", value, nested);
                    if (f == null || f instanceof NullField) {
                        continue;
                    }
                    if (type == null) {
                        type = f;
                    } else {
                        if (!type.equals(f)) {
                            if (ReflectionUtils.isPrimitiveTypeOrString(value.getClass())) {
                                type = new StringField("value");
                            } else {
                                type = new JsonField("value");
                                break;
                            }
                        }
                    }
                }
                if (type == null) {
                    type = new NullField("value");
                }
                map.innerType = type;
            }
            return map;
        }
    }

    public static class JsonField extends StringField {

        public JsonField(@NonNull String name) {
            super(name);
        }

        public JsonField(@NonNull StringField field) {
            super(field);
        }
    }

    public static class JsonToAvroSchema {
        public static Schema convert(@NonNull Map<String, Object> map,
                                     String namespace,
                                     @NonNull String name,
                                     boolean nested) throws Exception {
            Preconditions.checkArgument(!map.isEmpty());
            Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

            SchemaHelper.ObjectField field = SchemaHelper.ObjectField.parse(name, map, nested);
            if (!Strings.isNullOrEmpty(namespace))
                field.namespace(namespace);
            String avroSchema = field.avroSchema();
            return new Schema.Parser().parse(avroSchema);
        }

        public static Schema convert(@NonNull String json,
                                     String namespace,
                                     @NonNull String name,
                                     boolean nested) throws Exception {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(json));
            Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(json, Map.class);

            SchemaHelper.ObjectField field = SchemaHelper.ObjectField.parse(name, map, nested);
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
            DefaultLogger.LOGGER.debug(String.format("\nJSON: [\n%s\n]", json));
            SchemaHelper.ObjectField field =
                    SchemaHelper.ObjectField.parse(data.getClass().getSimpleName(), map, true);
            field.namespace(data.getClass().getCanonicalName());
            String avroSchema = field.avroSchema();
            return new Schema.Parser().parse(avroSchema);
        }
    }
}
