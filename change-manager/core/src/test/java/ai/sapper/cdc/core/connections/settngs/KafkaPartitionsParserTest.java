package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class KafkaPartitionsParserTest {

    @Test
    void parse() {
        try {
            KafkaPartitionsParser parser = new KafkaPartitionsParser();
            String s = "10";
            List<Integer> parts = parser.parse(s);
            assertEquals(10, parts.get(0));
            s = "10;20";
            parts = parser.parse(s);
            assertEquals(10, parts.get(0));
            assertEquals(20, parts.get(1));
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}