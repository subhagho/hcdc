package ai.sapper.hcdc.utils;

import ai.sapper.cdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CSVDataReaderTest {
    private static final String __INPUT_FILE = "src/test/resources/movies.csv";

    @Test
    void read() {
        try {
            CSVDataReader reader = new CSVDataReader(__INPUT_FILE, Character.MIN_VALUE);
            List<List<String>> records = reader.read();
            assertNotNull(records);
            assertTrue(records.size() > 0);
            for (List<String> record : records) {
                DefaultLogger.LOGGER.info(record.toString());
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}