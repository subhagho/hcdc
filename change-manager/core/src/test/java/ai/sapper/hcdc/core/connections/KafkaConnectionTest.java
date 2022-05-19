package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.DefaultLogger;
import ai.sapper.hcdc.core.connections.impl.BasicKafkaConsumer;
import ai.sapper.hcdc.core.connections.impl.BasicKafkaProducer;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class KafkaConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __PRODUCER_NAME = "test-kafka-producer";
    private static final String __CONSUMER_NAME = "test-kafka-consumer";
    private static final String __MESSAGE_FILE = "src/test/resources/test-message.txt";
    private static XMLConfiguration xmlConfiguration = null;

    private static ConnectionManager manager = new ConnectionManager();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        manager.init(xmlConfiguration, null);
    }

    @Test
    void test() {
        DefaultLogger.__LOG.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            BasicKafkaProducer producer = manager.getConnection(__PRODUCER_NAME, BasicKafkaProducer.class);
            producer.connect();

            BasicKafkaConsumer consumer = manager.getConnection(__CONSUMER_NAME, BasicKafkaConsumer.class);
            consumer.connect();

            File mf = new File(__MESSAGE_FILE);
            assertTrue(mf.exists());
            StringBuilder text = new StringBuilder();
            FileReader fr = new FileReader(mf);   //reads the file
            try (BufferedReader reader = new BufferedReader(fr)) {  //creates a buffering character input stream
                String line;
                while ((line = reader.readLine()) != null) {
                    text.append(line).append("\n");
                }
            }
            Map<String, Integer> sentIds = new HashMap<>();
            for (int ii = 0; ii < 10; ii++) {
                String mid = UUID.randomUUID().toString();
                String mesg = String.format("[%s] %s", mid, text.toString());
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(producer.topic(), mid, mesg.getBytes(StandardCharsets.UTF_8));
                RecordMetadata metadata = producer.producer().send(record).get();
                DefaultLogger.__LOG.debug(String.format("Message ID: %s [%s]", mid, metadata.toString()));
                sentIds.put(mid, mesg.length());
            }
            producer.producer().flush();

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.consumer().poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : records) {
                    String mid = record.key();
                    String mesg = new String(record.value());

                    assertTrue(sentIds.containsKey(mid));
                    int len = sentIds.get(mid);
                    assertEquals(len, mesg.length());
                }
                DefaultLogger.__LOG.info(String.format("Fetched [%d] messages...", records.count()));
            }
        } catch (Throwable t) {
            DefaultLogger.__LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}