package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.kafka.BasicKafkaConsumer;
import ai.sapper.cdc.core.connections.kafka.BasicKafkaProducer;
import ai.sapper.cdc.core.utils.DemoEnv;
import com.google.common.base.Preconditions;
import lombok.NonNull;
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
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class KafkaConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __PRODUCER_NAME = "test-kafka-producer";
    private static final String __CONSUMER_NAME = "test-kafka-consumer";
    private static final String __MESSAGE_FILE = "src/test/resources/test-message.xml";
    private static XMLConfiguration xmlConfiguration = null;

    private static ConnectionManager manager = new ConnectionManager();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        DemoEnv env = new DemoEnv();
        env.init(xmlConfiguration);
        manager = env.connectionManager();
    }

    @Test
    void test() {
        DefaultLogger.LOGGER.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            BasicKafkaProducer producer = manager.getConnection(__PRODUCER_NAME, BasicKafkaProducer.class);
            producer.connect();

            BasicKafkaConsumer consumer = manager.getConnection(__CONSUMER_NAME, BasicKafkaConsumer.class);
            consumer.connect();

            manager.save(producer);
            manager.save(consumer);

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
            Thread thread = new Thread(new ConsumerThread(manager));
            thread.start();

            for (int ii = 0; ii < 10; ii++) {
                String mid = UUID.randomUUID().toString();
                String mesg = String.format("[%s] %s", mid, text.toString());
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(producer.topic(), mid, mesg.getBytes(StandardCharsets.UTF_8));
                RecordMetadata metadata = producer.producer().send(record).get();
                DefaultLogger.LOGGER.debug("Record sent with key " + mid + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());

                sentIds.put(mid, mesg.length());
            }
            producer.producer().flush();
            thread.join();

        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    public static class ConsumerThread implements Runnable {
        private ConnectionManager manager;

        public ConsumerThread(@NonNull ConnectionManager manager) {
            this.manager = manager;
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run() {
            try {
                BasicKafkaConsumer consumer = manager.getConnection(__CONSUMER_NAME, BasicKafkaConsumer.class);

                while (true) {
                    ConsumerRecords<String, byte[]> records = consumer.consumer().poll(Duration.ofMillis(10000));
                    for (ConsumerRecord<String, byte[]> record : records) {
                        String mid = record.key();
                        String mesg = new String(record.value());
                        DefaultLogger.LOGGER.debug(String.format("[TOPIC=%s, PARTITION=%d][KEY=%s, OFFSET=%d]: %s", record.topic(),
                                record.partition(),
                                record.key(),
                                record.offset(),
                                mesg));
                        System.out.printf("[KEY=%s]: %s%n", record.key(), mesg);

                    }
                    DefaultLogger.LOGGER.info(String.format("Fetched [%d] messages...", records.count()));
                    if (records.count() <= 0) break;
                }
            } catch (Throwable t) {
                DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            }
        }
    }
}