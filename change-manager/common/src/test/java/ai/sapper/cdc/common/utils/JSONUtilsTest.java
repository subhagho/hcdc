package ai.sapper.cdc.common.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JSONUtilsTest {

    @Test
    void isJson() {
        try {
            String v1 = "128379812";
            assertFalse(JSONUtils.isJson(v1));
            String v2 = "{\n" +
                    "    \"@class\" : \"ai.sapper.cdc.entity.DataType\",\n" +
                    "    \"name\" : \"OBJECT\",\n" +
                    "    \"javaType\" : \"java.lang.Object\",\n" +
                    "    \"jdbcType\" : 2002\n" +
                    "  }";
            assertTrue(JSONUtils.isJson(v2));
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex);
        }
    }
}