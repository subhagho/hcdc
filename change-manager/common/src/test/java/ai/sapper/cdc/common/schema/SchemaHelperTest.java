package ai.sapper.cdc.common.schema;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

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
            SchemaHelper.ObjectCache cache = new SchemaHelper.ObjectCache();
            SchemaHelper.Field field = SchemaHelper.ObjectField.parse("", map, true, cache);
            assertNotNull(field);
            DefaultLogger.LOGGER.info(String.format("\nSCHEMA: [%s]\n", field.avroSchema(cache)));
        } catch (Exception ex) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(ex));
            fail(ex);
        }
    }

    @Test
    void testPOJO() {
        try {

            Profile profile = new Profile();
            profile.id = "85596a2c-a467-4bb5-8de8-9efaa84dffd7";
            profile.profileUrl = "https://qualitiesijjo38n.af";
            profile.name = "Luna";
            profile.contact = new Contact();
            profile.contact.emailId = "ivelisse_palermo93@studies.gyn";
            profile.contact.fistName = "Delphia";
            profile.contact.lastName = "Megann";
            profile.contact.id = "21e9b6ea-b82c-4b32-8116-b6d266980921";
            profile.contact.updateTimestamp = System.currentTimeMillis();
            profile.contact.phoneNumbers = new ArrayList<>();
            profile.contact.phoneNumbers.add("+91 9608687009");
            profile.contact.phoneNumbers.add("+91 6068798796");
            profile.contact.phoneNumbers.add("+91 7750643753");
            profile.contact.office = new Address();
            profile.contact.office.addressLine1 = "Proposals St 43,";
            profile.contact.office.addressLine2 = "Sudan St 9855,";
            profile.contact.office.city = "Minsk";
            profile.contact.office.state = "CA";
            profile.contact.office.zipCode = "0278573";
            profile.contact.residence = new Address();
            profile.contact.residence.addressLine1 = "Latex St 2478,";
            profile.contact.residence.city = "Marblemount";
            profile.contact.residence.country = "Israel";
            profile.contact.residence.zipCode = "990104";

            Schema schema = SchemaHelper.POJOToAvroSchema.convert(profile);
            DefaultLogger.LOGGER.info(String.format("\nSCHEMA: [%s]\n", schema.toString()));
        } catch (Exception ex) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(ex));
            fail(ex);
        }
    }

    @Test
    void testMap() {
        try {
            Random rnd = new Random(System.currentTimeMillis());
            MapTest mt = new MapTest();
            mt.id = "4c48ca61-b44f-490a-8409-eac4704358f3";
            mt.date = "1975-12-18 18:38:52";
            for (int ii = 0; ii < 10; ii++) {
                mt.values.add(UUID.randomUUID().toString());
            }
            for (int ii = 0; ii < 10; ii++) {
                mt.map.put(UUID.randomUUID().toString(), rnd.nextDouble());
            }
            String json = JSONUtils.asString(mt, MapTest.class);
            DefaultLogger.LOGGER.debug(json);
            SchemaHelper.ObjectCache cache = new SchemaHelper.ObjectCache();
            Map<String, Object> jsonMap = JSONUtils.read(json, Map.class);
            SchemaHelper.Field field = SchemaHelper.ObjectField.parse("", jsonMap, true, cache);
            assertNotNull(field);

            String schema = field.avroSchema(cache);
            DefaultLogger.LOGGER.info(String.format("\nSCHEMA: [%s]\n", schema));
        } catch (Exception ex) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(ex));
            fail(ex);
        }
    }

    @Getter
    @Setter
    private static class MapTest {
        private String id;
        private String date;
        private List<String> values = new ArrayList<>();
        private Map<String, Double> map = new HashMap<>();
    }

    @Getter
    @Setter
    private static class Address {
        private String addressLine1;
        private String addressLine2;
        private String city;
        private String state;
        private String zipCode;
        private String country;
    }

    @Getter
    @Setter
    public static class Contact {
        private String id;
        private long updateTimestamp;
        private String fistName;
        private String lastName;
        private String emailId;
        private List<String> phoneNumbers;
        private Address residence;
        private Address office;
    }

    @Getter
    @Setter
    public static class Profile {
        private String id;
        private String name;
        private String profileUrl;
        private Contact contact;
    }

}