package ai.sapper.cdc.core.schema;

import ai.sapper.cdc.common.schema.AvroSchema;
import ai.sapper.cdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AvroSchemaTest {
    private static final String SCHEMA_1 = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"record\",\n" +
            "  \"namespace\" : \"ai.sapper.hcdc\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"genres\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"movieId\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"title\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  } ]\n" +
            "}";

    private static final String SCHEMA_2 = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"record\",\n" +
            "  \"namespace\" : \"ai.sapper.hcdc\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"genres\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"movieId\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"ADDED_COLUMN_UUID\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"title\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  } ]\n" +
            "}";

    @Test
    void compare() {
        try {
            AvroSchema schema1 = new AvroSchema().withSchemaStr(SCHEMA_1);
            AvroSchema schema2 = new AvroSchema().withSchemaStr(SCHEMA_2);
            AvroSchema schema3 = new AvroSchema().withSchemaStr(SCHEMA_1);

            assertFalse(schema1.compare(schema2));
            assertTrue(schema1.compare(schema3));

        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}