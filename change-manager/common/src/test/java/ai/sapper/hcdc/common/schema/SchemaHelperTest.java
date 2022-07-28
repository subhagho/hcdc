package ai.sapper.hcdc.common.schema;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaHelperTest {
    @Test
    void testJsonMap() {
        try {
            String json = "{\n" +
                    "\t\t\"id\": \"0001\",\n" +
                    "\t\t\"type\": \"donut\",\n" +
                    "\t\t\"name\": \"Cake\",\n" +
                    "\t\t\"ppu\": 0.55,\n" +
                    "\t\t\"batters\":\n" +
                    "\t\t\t{\n" +
                    "\t\t\t\t\"batter\":\n" +
                    "\t\t\t\t\t[\n" +
                    "\t\t\t\t\t\t{ \"id\": \"1001\", \"type\": \"Regular\" },\n" +
                    "\t\t\t\t\t\t{ \"id\": \"1002\", \"type\": \"Chocolate\" },\n" +
                    "\t\t\t\t\t\t{ \"id\": \"1003\", \"type\": \"Blueberry\" },\n" +
                    "\t\t\t\t\t\t{ \"id\": \"1004\", \"type\": \"Devil's Food\" }\n" +
                    "\t\t\t\t\t]\n" +
                    "\t\t\t},\n" +
                    "\t\t\"topping\":\n" +
                    "\t\t\t[\n" +
                    "\t\t\t\t{ \"id\": \"5001\", \"type\": \"None\" },\n" +
                    "\t\t\t\t{ \"id\": \"5002\", \"type\": \"Glazed\" },\n" +
                    "\t\t\t\t{ \"id\": \"5005\", \"type\": \"Sugar\" },\n" +
                    "\t\t\t\t{ \"id\": \"5007\", \"type\": \"Powdered Sugar\" },\n" +
                    "\t\t\t\t{ \"id\": \"5006\", \"type\": \"Chocolate with Sprinkles\" },\n" +
                    "\t\t\t\t{ \"id\": \"5003\", \"type\": \"Chocolate\" },\n" +
                    "\t\t\t\t{ \"id\": \"5004\", \"type\": \"Maple\" }\n" +
                    "\t\t\t]\n" +
                    "\t}";
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(json, Map.class);
            assertNotNull(map);
        } catch (Exception ex) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(ex));
            fail(ex);
        }
    }
}