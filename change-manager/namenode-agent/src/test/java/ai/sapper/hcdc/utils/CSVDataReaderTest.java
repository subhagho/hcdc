package ai.sapper.hcdc.utils;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CSVDataReaderTest {
    private static final String __INPUT_FILE = "src/test/resources/movies.csv";

    @Test
    void read() {
        try {
            CSVDataReader reader = new CSVDataReader(__INPUT_FILE, Character.MIN_VALUE);
            reader.read();
            assertNotNull(reader.records());
            assertTrue(reader.records().size() > 0);
            for (List<String> record : reader.records()) {
                DefaultLogger.__LOG.info(record.toString());
            }
        } catch (Exception ex) {
            DefaultLogger.__LOG.debug(DefaultLogger.stacktrace(ex));
            fail(ex);
        }
    }
}