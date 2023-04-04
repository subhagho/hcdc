package ai.sapper.cdc.common.utils;


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright (c) $year
 * Date: 3/2/19 12:10 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.io.File;
import java.lang.reflect.*;
import java.util.*;

/**
 * Utility functions to help with Getting/Setting Object/Field values using Reflection.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * <p>
 * 11:10:30 AM
 */
public class ReflectionUtils {

    /**
     * Get the nested Field for the dot notation name.
     * (name="a.b.c")
     *
     * @param type - Class type to search the field for.
     * @param name - Nested field name.
     * @return - Key Value Pair (Class<?>, Field)
     */
    public static KeyValuePair<String, Field> findNestedField(@NonNull Class<?> type,
                                                              @NonNull String name) {
        String[] parts = name.split("\\.");
        Class<?> ntype = type;
        int index = 0;
        KeyValuePair<String, Field> kv = null;
        while (index < parts.length) {
            Field field = findField(ntype, parts[index]);
            if (field == null) {
                kv = null;
                break;
            }
            if (kv == null) {
                kv = new KeyValuePair<>();
            }
            kv.key(name);
            kv.value(field);
            ntype = field.getType();
            index++;
        }
        return kv;
    }

    /**
     * Find the field with the specified name in this type or a parent type.
     *
     * @param type - Class to find the field in.
     * @param name - Field name.
     * @return - Found Field or NULL
     */
    public static Field findField(@NonNull Class<?> type,
                                  @NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        if (name.indexOf('.') > 0) {
            String[] parts = name.split("\\.");
            int indx = 0;
            Class<?> ct = type;
            Field f = null;
            while (indx < parts.length) {
                f = findField(ct, parts[indx]);
                if (f == null) break;
                ct = f.getType();
                if (implementsInterface(List.class, ct)) {
                    ct = getGenericListType(f);
                } else if (implementsInterface(Set.class, ct)) {
                    ct = getGenericSetType(f);
                }
                indx++;
            }
            return f;
        } else {
            Field[] fields = type.getDeclaredFields();
            if (fields != null && fields.length > 0) {
                for (Field field : fields) {
                    if (field.getName().compareTo(name) == 0) {
                        return field;
                    }
                }
            }
            Class<?> parent = type.getSuperclass();
            if (parent != null && !parent.equals(Object.class)) {
                return findField(parent, name);
            }
        }
        return null;
    }

    /**
     * Recursively get all the public methods declared for a type.
     *
     * @param type - Type to fetch fields for.
     * @return - Array of all defined methods.
     */
    public static Method[] getAllMethods(@NonNull Class<?> type) {
        List<Method> methods = new ArrayList<>();
        getMethods(type, methods);
        if (!methods.isEmpty()) {
            Method[] ma = new Method[methods.size()];
            for (int ii = 0; ii < methods.size(); ii++) {
                ma[ii] = methods.get(ii);
            }
            return ma;
        }
        return null;
    }

    /**
     * Get all public methods declared for this type and add them to the list passed.
     *
     * @param type    - Type to get methods for.
     * @param methods - List of methods.
     */
    private static void getMethods(Class<?> type, List<Method> methods) {
        Method[] ms = type.getDeclaredMethods();
        if (ms.length > 0) {
            for (Method m : ms) {
                if (m != null && Modifier.isPublic(m.getModifiers()))
                    methods.add(m);
            }
        }
        Class<?> st = type.getSuperclass();
        if (st != null && !st.equals(Object.class)) {
            getMethods(st, methods);
        }
    }

    /**
     * Recursively get all the declared fields for a type.
     *
     * @param type - Type to fetch fields for.
     * @return - Array of all defined fields.
     */
    public static Field[] getAllFields(@NonNull Class<?> type) {
        List<Field> fields = new ArrayList<>();
        getFields(type, fields);
        if (!fields.isEmpty()) {
            Field[] fa = new Field[fields.size()];
            for (int ii = 0; ii < fields.size(); ii++) {
                fa[ii] = fields.get(ii);
            }
            return fa;
        }
        return null;
    }

    public static Map<String, Field> getFieldsMap(@NonNull Class<?> type) {
        Field[] fields = getAllFields(type);
        if (fields != null && fields.length > 0) {
            Map<String, Field> map = new HashMap<>();
            for (Field field : fields) {
                map.put(field.getName(), field);
            }
            return map;
        }
        return null;
    }

    /**
     * Get fields declared for this type and add them to the list passed.
     *
     * @param type   - Type to get fields for.
     * @param fields - List of fields.
     */
    private static void getFields(Class<?> type, List<Field> fields) {
        Field[] fs = type.getDeclaredFields();
        if (fs.length > 0) {
            for (Field f : fs) {
                if (f != null)
                    fields.add(f);
            }
        }
        Class<?> st = type.getSuperclass();
        if (st != null && !st.equals(Object.class)) {
            getFields(st, fields);
        }
    }

    /**
     * Get the String value of the field in the object passed.
     *
     * @param o     - Object to extract field value from.
     * @param field - Field to extract.
     * @return - String value.
     * @throws Exception
     */
    public static String strinfigy(@NonNull Object o, @NonNull Field field)
            throws Exception {
        Preconditions.checkArgument(o != null);
        Preconditions.checkArgument(field != null);

        Object v = getFieldValue(o, field);
        if (v != null) {
            return String.valueOf(v);
        }
        return null;
    }

    public static Object getNestedFieldValue(@NonNull Object source,
                                             @NonNull String name) throws Exception {
        String[] parts = name.split("\\.");
        Object value = source;
        Class<?> type = source.getClass();
        int index = 0;
        while (index < parts.length) {
            Field field = findField(type, parts[index]);
            if (field == null) {
                throw new Exception(String.format("Field not found. [type=%s][field=%s]",
                        type.getCanonicalName(), parts[index]));
            }
            value = getFieldValue(value, field);
            if (value == null) {
                break;
            }
            type = field.getType();
            index++;
        }
        return value;
    }

    /**
     * Get the value of the specified field from the object passed.
     * This assumes standard bean Getters/Setters.
     *
     * @param o     - Object to get field value from.
     * @param field - Field value to extract.
     * @return - Field value.
     * @throws Exception
     */
    public static Object getFieldValue(@NonNull Object o, @NonNull Field field) throws Exception {
        return getFieldValue(o, field, false);
    }

    public static Object getFieldValue(@NonNull Object o, @NonNull Field field, boolean ignore)
            throws Exception {
        String method = "get" + StringUtils.capitalize(field.getName());

        Method m = MethodUtils.getAccessibleMethod(o.getClass(), method);
        if (m == null) {
            method = field.getName();
            m = MethodUtils.getAccessibleMethod(o.getClass(), method);
        }

        if (m == null) {
            Class<?> type = field.getType();
            if (type.equals(boolean.class) || type.equals(Boolean.class)) {
                method = "is" + StringUtils.capitalize(field.getName());
                m = MethodUtils.getAccessibleMethod(o.getClass(), method);
            }
        }

        if (m == null)
            if (!ignore)
                throw new Exception("No accessible method found for field. [field="
                        + field.getName() + "][class="
                        + o.getClass().getCanonicalName() + "]");
            else return null;

        return MethodUtils.invokeMethod(o, method);
    }

    /**
     * Check is the field value can be converted to a String value.
     *
     * @param field - Field to check type for.
     * @return - Can convert to String?
     */
    public static boolean canStringify(@NonNull Field field) {
        if (field.isEnumConstant() || field.getType().isEnum())
            return true;
        if (isPrimitiveTypeOrClass(field))
            return true;
        if (field.getType().equals(String.class))
            return true;
        if (field.getType().equals(Date.class))
            return true;
        return false;
    }

    /**
     * Set the value of a primitive attribute for the specified object.
     *
     * @param value  - Value to set.
     * @param source - Object to set the attribute value.
     * @param f      - Field to set value for.
     * @throws Exception
     */
    public static void setPrimitiveValue(@NonNull String value,
                                         @NonNull Object source,
                                         @NonNull Field f) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        Class<?> type = f.getType();
        if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            setBooleanValue(source, f, value);
        } else if (type.equals(short.class) || type.equals(Short.class)) {
            setShortValue(source, f, value);
        } else if (type.equals(int.class) || type.equals(Integer.class)) {
            setIntValue(source, f, value);
        } else if (type.equals(float.class) || type.equals(Float.class)) {
            setFloatValue(source, f, value);
        } else if (type.equals(double.class) || type.equals(Double.class)) {
            setDoubleValue(source, f, value);
        } else if (type.equals(long.class) || type.equals(Long.class)) {
            setLongValue(source, f, value);
        } else if (type.equals(char.class) || type.equals(Character.class)) {
            setCharValue(source, f, value);
        } else if (type.equals(Class.class)) {
            setClassValue(source, f, value);
        } else if (type.equals(String.class)) {
            setStringValue(source, f, value);
        }
    }

    public static void setPrimitiveValue(@NonNull Object value,
                                         @NonNull Object source,
                                         @NonNull Field f) throws Exception {
        Class<?> type = f.getType();
        if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            setBooleanValue(source, f, value);
        } else if (type.equals(short.class) || type.equals(Short.class)) {
            setShortValue(source, f, value);
        } else if (type.equals(int.class) || type.equals(Integer.class)) {
            setIntValue(source, f, value);
        } else if (type.equals(float.class) || type.equals(Float.class)) {
            setFloatValue(source, f, value);
        } else if (type.equals(double.class) || type.equals(Double.class)) {
            setDoubleValue(source, f, value);
        } else if (type.equals(long.class) || type.equals(Long.class)) {
            setLongValue(source, f, value);
        } else if (type.equals(char.class) || type.equals(Character.class)) {
            setCharValue(source, f, value);
        } else if (type.equals(Class.class)) {
            setClassValue(source, f, value);
        } else if (type.equals(String.class)) {
            setStringValue(source, f, value);
        }
    }

    /**
     * Set the value of the field by converting the specified String value to the
     * required value type.
     *
     * @param value    - String value to set.
     * @param source   - Object to set the attribute value.
     * @param type     - Class type to set property for.
     * @param property - Property to set.
     * @return - True if value was set.
     */
    public static boolean setValueFromString(@NonNull String value,
                                             @NonNull Object source,
                                             @NonNull Class<?> type,
                                             @NonNull String property) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        Field f = findField(type, property);
        if (f != null) {
            try {
                setValueFromString(value, source, f);
                return true;
            } catch (ReflectionException re) {
                DefaultLogger.LOGGER.error(re.getLocalizedMessage(), re);
            }
        }
        return false;
    }

    /**
     * Set the value of the field by converting the specified String value to the
     * required value type.
     *
     * @param value  - String value to set.
     * @param source - Object to set the attribute value.
     * @param f      - Field to set value for.
     * @return - Updated object instance.
     * @throws ReflectionException
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Object setValueFromString(@NonNull String value,
                                            @NonNull Object source,
                                            @NonNull Field f) throws
            ReflectionException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        try {
            Object retV = value;
            Class<?> type = f.getType();
            if (ReflectionUtils
                    .isPrimitiveTypeOrClass(f)) {
                ReflectionUtils
                        .setPrimitiveValue(value, source, f);
            } else if (type.equals(String.class)) {
                ReflectionUtils
                        .setStringValue(source, f, value);
            } else if (type.isEnum()) {
                Class<Enum> et = (Class<Enum>) type;
                Object ev = Enum.valueOf(et, value);
                ReflectionUtils
                        .setObjectValue(source, f, ev);
                retV = ev;
            } else if (type.equals(File.class)) {
                File file = new File(value);
                ReflectionUtils
                        .setObjectValue(source, f, file);
                retV = file;
            } else if (type.equals(Class.class)) {
                Class<?> cls = Class.forName(value.trim());
                ReflectionUtils
                        .setObjectValue(source, f, cls);
                retV = cls;
            } else {
                Class<?> cls = Class.forName(value.trim());
                if (type.isAssignableFrom(cls)) {
                    Object o = cls.newInstance();
                    ReflectionUtils
                            .setObjectValue(source, f, o);
                    retV = o;
                } else {
                    throw new InstantiationException(
                            "Cannot create instance of type [type="
                                    + cls.getCanonicalName()
                                    + "] and assign to field [field="
                                    + f.getName() + "]");
                }
            }
            return retV;
        } catch (Exception e) {
            throw new ReflectionException(
                    "Error setting object value : [type="
                            + source.getClass().getCanonicalName() + "][field="
                            + f.getName() + "]",
                    e);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Object setValue(@NonNull Object value,
                                  @NonNull Object source,
                                  @NonNull Field f) throws
            ReflectionException {
        try {
            Object retV = value;
            Class<?> type = f.getType();
            if (ReflectionUtils
                    .isPrimitiveTypeOrClass(f)) {
                ReflectionUtils
                        .setPrimitiveValue(value, source, f);
            } else if (type.equals(String.class)) {
                ReflectionUtils
                        .setStringValue(source, f, value);
            } else if (type.isEnum()) {
                Object ev = null;
                if (value instanceof Enum) {
                    ev = value;
                } else if (value instanceof String) {
                    Class<Enum> et = (Class<Enum>) type;
                    ev = Enum.valueOf(et, (String) value);
                } else {
                    throw new ReflectionException(String.format("Failed to convert to Enum[%s]. [type=%s]",
                            type.getCanonicalName(),
                            value.getClass().getCanonicalName()));
                }

                ReflectionUtils
                        .setObjectValue(source, f, ev);
                retV = ev;
            } else if (type.equals(File.class)) {
                File file = null;
                if (value instanceof File) {
                    file = (File) value;
                } else if (value instanceof String) {
                    file = new File((String) value);
                } else {
                    throw new ReflectionException(String.format("Failed to convert to File. [type=%s]",
                            value.getClass().getCanonicalName()));
                }
                ReflectionUtils
                        .setObjectValue(source, f, file);
                retV = file;
            } else if (type.equals(Class.class)) {
                ReflectionUtils
                        .setClassValue(source, f, value);
            } else if (value instanceof String) {
                String v = (String) value;
                Class<?> cls = Class.forName(v.trim());
                if (type.isAssignableFrom(cls)) {
                    Object o = cls.getConstructor().newInstance();
                    ReflectionUtils
                            .setObjectValue(source, f, o);
                    retV = o;
                } else {
                    throw new InstantiationException(
                            "Cannot create instance of type [type="
                                    + cls.getCanonicalName()
                                    + "] and assign to field [field="
                                    + f.getName() + "]");
                }
            } else {
                ReflectionUtils
                        .setObjectValue(source, f, value);
            }
            return retV;
        } catch (Exception e) {
            throw new ReflectionException(
                    "Error setting object value : [type="
                            + source.getClass().getCanonicalName() + "][field="
                            + f.getName() + "]",
                    e);
        }
    }

    /**
     * Set the value of the specified field in the object to the value passed.
     *
     * @param o     - Object to set value for.
     * @param f     - Field to set value for.
     * @param value - Value to set to.
     * @throws Exception
     */
    public static void setObjectValue(@NonNull Object o,
                                      @NonNull Field f,
                                      Object value)
            throws Exception {

        Method m = getSetter(o.getClass(), f);

        if (m == null)
            throw new Exception("No accessable method found for field. [field="
                    + f.getName() + "][class=" +
                    o.getClass().getCanonicalName()
                    + "]");
        MethodUtils.invokeMethod(o, m.getName(), value);
    }


    public static Method getSetter(Class<?> type, Field f) {
        Preconditions.checkArgument(f != null);

        String method = "set" + StringUtils.capitalize(f.getName());
        Method m = MethodUtils.getAccessibleMethod(type, method,
                f.getType());
        if (m == null) {
            method = f.getName();
            m = MethodUtils.getAccessibleMethod(type, method,
                    f.getType());
        }
        return m;
    }

    /**
     * Set the value of the specified field in the object to the value passed.
     *
     * @param o        - Object to set value for.
     * @param property - Property name to set value for.
     * @param type     - Class type
     * @param value    - Value to set to.
     * @return - True, if value set.
     * @throws Exception
     */
    public static boolean setObjectValue(@NonNull Object o,
                                         @NonNull String property,
                                         @NonNull Class<?> type,
                                         Object value)
            throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));

        Field f = type.getField(property);
        if (f == null) {
            return false;
        }

        String method = "set" + StringUtils.capitalize(f.getName());
        Method m = MethodUtils.getAccessibleMethod(o.getClass(), method,
                f.getType());
        if (m == null) {
            method = f.getName();
            m = MethodUtils.getAccessibleMethod(o.getClass(), method,
                    f.getType());
        }

        if (m == null)
            return false;

        MethodUtils.invokeMethod(o, method, value);
        return true;
    }

    /**
     * Set the value of the field to the passed String value.
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setStringValue(@NonNull Object o,
                                      @NonNull Field f,
                                      Object value)
            throws Exception {
        setObjectValue(o, f, value);
    }

    /**
     * Set the value of the field to boolean value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setBooleanValue(@NonNull Object o,
                                       @NonNull Field f,
                                       @NonNull Object value)
            throws Exception {
        Boolean bv = null;
        if (isBoolean(value.getClass())) {
            bv = (Boolean) value;
        } else if (value instanceof String) {
            bv = Boolean.parseBoolean((String) value);
        } else {
            throw new Exception(String.format("Failed to convert to boolean. [type=%s]",
                    value.getClass().getCanonicalName()));
        }
        setObjectValue(o, f, bv);
    }

    /**
     * Set the value of the field to Short value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setShortValue(@NonNull Object o,
                                     @NonNull Field f,
                                     @NonNull Object value)
            throws Exception {
        Short sv = null;
        if (isShort(value.getClass())) {
            sv = (Short) value;
        } else if (value instanceof String) {
            sv = Short.parseShort((String) value);
        } else {
            throw new Exception(String.format("Failed to convert to short. [type=%s]",
                    value.getClass().getCanonicalName()));
        }
        setObjectValue(o, f, sv);
    }

    /**
     * Set the value of the field to Integer value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setIntValue(@NonNull Object o,
                                   @NonNull Field f,
                                   @NonNull Object value)
            throws Exception {
        Integer iv = null;
        if (isInt(value.getClass())) {
            iv = (Integer) value;
        } else if (value instanceof String) {
            iv = Integer.parseInt((String) value);
        } else {
            throw new Exception(String.format("Failed to convert to integer. [type=%s]",
                    value.getClass().getCanonicalName()));
        }
        setObjectValue(o, f, iv);
    }

    /**
     * Set the value of the field to Long value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setLongValue(@NonNull Object o,
                                    @NonNull Field f,
                                    @NonNull Object value)
            throws Exception {
        Long lv = null;
        if (isLong(value.getClass())) {
            lv = (Long) value;
        } else if (value instanceof String) {
            lv = Long.parseLong((String) value);
        } else {
            throw new Exception(String.format("Failed to convert to long. [type=%s]",
                    value.getClass().getCanonicalName()));
        }
        setObjectValue(o, f, lv);
    }

    /**
     * Set the value of the field to Float value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setFloatValue(@NonNull Object o,
                                     @NonNull Field f,
                                     @NonNull Object value)
            throws Exception {
        Float fv = null;
        if (isFloat(value.getClass())) {
            fv = (Float) value;
        } else if (value instanceof String) {
            fv = Float.parseFloat((String) value);
        } else {
            throw new Exception(String.format("Failed to convert to float. [type=%s]",
                    value.getClass().getCanonicalName()));
        }
        setObjectValue(o, f, fv);
    }

    /**
     * Set the value of the field to Double value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setDoubleValue(@NonNull Object o,
                                      @NonNull Field f,
                                      @NonNull Object value)
            throws Exception {
        Double dv = null;
        if (isDouble(value.getClass())) {
            dv = (Double) value;
        } else if (value instanceof String) {
            dv = Double.parseDouble((String) value);
        } else {
            throw new Exception(String.format("Failed to convert to double. [type=%s]",
                    value.getClass().getCanonicalName()));
        }
        setObjectValue(o, f, dv);
    }

    /**
     * Set the value of the field to Char value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setCharValue(@NonNull Object o,
                                    @NonNull Field f,
                                    @NonNull Object value)
            throws Exception {
        Character cv = null;
        if (isChar(value.getClass())) {
            cv = (Character) value;
        } else if (value instanceof String) {
            cv = ((String) value).charAt(0);
        } else {
            throw new Exception(String.format("Failed to convert to char. [type=%s]",
                    value.getClass().getCanonicalName()));
        }
        setObjectValue(o, f, cv);
    }

    /**
     * Set the value of the field to Class value by converting the passed string..
     *
     * @param o     - Object to set the value for.
     * @param f     - Field to set the value for.
     * @param value - Value to set.
     * @throws Exception
     */
    public static void setClassValue(@NonNull Object o,
                                     @NonNull Field f,
                                     @NonNull Object value)
            throws Exception {
        Class<?> cv = null;
        if (value instanceof Class) {
            cv = (Class<?>) value;
        } else if (value instanceof String) {
            Class.forName((String) value);
        } else {
            throw new Exception(String.format("Failed to convert to class. [type=%s]",
                    value.getClass().getCanonicalName()));
        }
        setObjectValue(o, f, cv);
    }

    /**
     * Check if the specified field is a primitive or primitive type class.
     *
     * @param field - Field to check primitive for.
     * @return - Is primitive?
     */
    public static boolean isPrimitiveTypeOrClass(@NonNull Field field) {
        Class<?> type = field.getType();
        return isPrimitiveTypeOrClass(type);
    }

    /**
     * Check if the specified type is a primitive or primitive type class.
     *
     * @param type - Field to check primitive for.
     * @return - Is primitive?
     */
    public static boolean isPrimitiveTypeOrClass(@NonNull Class<?> type) {
        if (isNumericType(type)) return true;
        if (type.equals(Boolean.class) || type.equals(boolean.class)) return true;
        return type.equals(Class.class);
    }

    /**
     * Check if the specified field is a primitive or primitive type class or String.
     *
     * @param field - Field to check primitive/String for.
     * @return - Is primitive or String?
     */
    public static boolean isPrimitiveTypeOrString(@NonNull Field field) {
        Class<?> type = field.getType();
        return isPrimitiveTypeOrString(type);
    }

    /**
     * Check if the specified type is a primitive or primitive type class or String.
     *
     * @param type - Field to check primitive/String for.
     * @return - Is primitive or String?
     */
    public static boolean isPrimitiveTypeOrString(@NonNull Class<?> type) {
        if (isPrimitiveTypeOrClass(type)) {
            return true;
        }
        if (type == String.class) {
            return true;
        }
        return false;
    }

    public static boolean isNumericType(@NonNull Class<?> type) {
        return type.equals(Short.class) || type.equals(short.class)
                || type.equals(Integer.class) || type.equals(int.class) ||
                type.equals(Long.class) || type.equals(long.class)
                || type.equals(Float.class) || type.equals(float.class) ||
                type.equals(Double.class) || type.equals(double.class)
                || type.equals(Character.class) || type.equals(char.class);
    }

    /**
     * Check if the parent type specified is an ancestor (inheritance) of the passed type.
     *
     * @param parent - Ancestor type to check.
     * @param type   - Inherited type
     * @return - Is Ancestor type?
     */
    public static boolean isSuperType(@NonNull Class<?> parent,
                                      @NonNull Class<?> type) {
        if (parent.equals(type)) {
            return true;
        } else if (type.equals(Object.class)) {
            return false;
        } else {
            Class<?> pp = type.getSuperclass();
            if (pp == null) {
                return false;
            }
            return isSuperType(parent, pp);
        }
    }

    /**
     * Check is the passed type (or its ancestor) implements the specified interface.
     *
     * @param intf - Interface type to check.
     * @param type - Type implementing expected interface.
     * @return - Implements Interface?
     */
    public static boolean implementsInterface(@NonNull Class<?> intf,
                                              @NonNull Class<?> type) {
        if (intf.equals(type)) {
            return true;
        }
        Class<?>[] intfs = type.getInterfaces();
        if (intfs != null && intfs.length > 0) {
            for (Class<?> itf : intfs) {
                if (isSuperType(intf, itf)) {
                    return true;
                }
            }
        }
        Class<?> parent = type.getSuperclass();
        if (parent != null && !parent.equals(Object.class)) {
            return implementsInterface(intf, parent);
        }
        return false;
    }

    /**
     * Get the Parameterized type of the Map key field specified.
     *
     * @param field - Field to extract the Parameterized type for.
     * @return - Parameterized type.
     */
    public static Class<?> getGenericMapKeyType(@NonNull Field field) {
        Preconditions
                .checkArgument(implementsInterface(Map.class, field.getType()));

        ParameterizedType ptype = (ParameterizedType) field.getGenericType();
        return (Class<?>) ptype.getActualTypeArguments()[0];
    }

    public static Class<?> getGenericMapKeyType(@NonNull Map<?, ?> mapObject) {
        if (!mapObject.isEmpty()) {
            for (Object key : mapObject.keySet()) {
                return key.getClass();
            }
        }
        return null;
    }

    public static Class<?> getGenericMapValueType(@NonNull Map<?, ?> mapObject) {
        if (!mapObject.isEmpty()) {
            for (Object value : mapObject.values()) {
                return value.getClass();
            }
        }
        return null;
    }

    /**
     * Get the Parameterized type of the Map value field specified.
     *
     * @param field - Field to extract the Parameterized type for.
     * @return - Parameterized type.
     */
    public static Class<?> getGenericMapValueType(@NonNull Field field) {
        Preconditions
                .checkArgument(implementsInterface(Map.class, field.getType()));

        ParameterizedType ptype = (ParameterizedType) field.getGenericType();
        return (Class<?>) ptype.getActualTypeArguments()[1];
    }

    /**
     * Get the Parameterized type of the List field specified.
     *
     * @param field - Field to extract the Parameterized type for.
     * @return - Parameterized type.
     */
    public static Class<?> getGenericListType(@NonNull Field field) {
        Preconditions
                .checkArgument(implementsInterface(List.class, field.getType()));

        ParameterizedType ptype = (ParameterizedType) field.getGenericType();
        return (Class<?>) ptype.getActualTypeArguments()[0];
    }

    /**
     * Get the Parameterized type of the Set field specified.
     *
     * @param field - Field to extract the Parameterized type for.
     * @return - Parameterized type.
     */
    public static Class<?> getGenericSetType(@NonNull Field field) {
        Preconditions
                .checkArgument(implementsInterface(Set.class, field.getType()));

        ParameterizedType ptype = (ParameterizedType) field.getGenericType();
        return (Class<?>) ptype.getActualTypeArguments()[0];
    }

    /**
     * Get the parsed value of the type specified from the
     * string value passed.
     *
     * @param type  - Required value type
     * @param value - Input String value
     * @return - Parsed Value.
     */
    @SuppressWarnings("unchecked")
    public static Object parseStringValue(Class<?> type, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            if (isPrimitiveTypeOrString(type)) {
                return parsePrimitiveValue(type, value);
            } else if (type.isEnum()) {
                Class<Enum> et = (Class<Enum>) type;
                return Enum.valueOf(et, value);
            }
        }
        return null;
    }

    /**
     * Get the value of the primitive type parsed from the string value.
     *
     * @param type  - Primitive Type
     * @param value - String value
     * @return - Parsed Value
     */
    private static Object parsePrimitiveValue(Class<?> type, String value) {
        if (type.equals(Boolean.class) || type.equals(boolean.class)) {
            return Boolean.parseBoolean(value);
        } else if (type.equals(Short.class) || type.equals(short.class)) {
            return Short.parseShort(value);
        } else if (type.equals(Integer.class) || type.equals(int.class)) {
            return Integer.parseInt(value);
        } else if (type.equals(Long.class) || type.equals(long.class)) {
            return Long.parseLong(value);
        } else if (type.equals(Float.class) || type.equals(float.class)) {
            return Float.parseFloat(value);
        } else if (type.equals(Double.class) || type.equals(double.class)) {
            return Double.parseDouble(value);
        } else if (type.equals(Character.class) || type.equals(char.class)) {
            return value.charAt(0);
        } else if (type.equals(Byte.class) || type.equals(byte.class)) {
            return Byte.parseByte(value);
        } else if (type.equals(String.class)) {
            return value;
        }
        return null;
    }

    public static Constructor<?> getConstructor(Class<?> type, Class<?>... args) throws ReflectionException, NoSuchMethodException {
        Constructor<?>[] constructors = type.getConstructors();
        int le = (args == null ? 0 : args.length);
        if (le == 0) {
            for (Constructor<?> ctor : constructors) {
                if (ctor.getGenericParameterTypes().length == le) {
                    return ctor;
                }
            }
        } else {
            return type.getDeclaredConstructor(args);
        }
        throw new ReflectionException(String.format("No matching constructor found. [type=%s][args=%s]",
                type.getCanonicalName(), (args == null ? "NONE" : Arrays.toString(args))));
    }

    public static boolean isBoolean(@NonNull Class<?> type) {
        return (type.equals(Boolean.TYPE) || type.equals(Boolean.class) || type.equals(boolean.class));
    }

    public static boolean isShort(@NonNull Class<?> type) {
        return (type.equals(Short.TYPE) || type.equals(Short.class) || type.equals(short.class));
    }

    public static boolean isInt(@NonNull Class<?> type) {
        return (type.equals(Integer.TYPE) || type.equals(Integer.class) || type.equals(int.class));
    }

    public static boolean isLong(@NonNull Class<?> type) {
        return (type.equals(Long.TYPE) || type.equals(Long.class) || type.equals(long.class));
    }

    public static boolean isFloat(@NonNull Class<?> type) {
        return (type.equals(Float.TYPE) || type.equals(Float.class) || type.equals(float.class));
    }

    public static boolean isDouble(@NonNull Class<?> type) {
        return (type.equals(Double.TYPE) || type.equals(Double.class) || type.equals(double.class));
    }

    public static boolean isByte(@NonNull Class<?> type) {
        return (type.equals(byte.class) || type.equals(Byte.class) || type.equals(Byte.TYPE));
    }

    public static boolean isChar(@NonNull Class<?> type) {
        return (type.equals(char.class) || type.equals(Character.class) || type.equals(Character.TYPE));
    }
}
