package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {

    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
    private KafkaConsumer<String, String> consumer;

    private CountDownLatch latch; // to handle concurrency, make sure the task waits for other tasks before it starts

    public ConsumerRunnable(String bootstrapServer,
                            String groupId,
                            String topic,
                            CountDownLatch latch) {
        this.latch = latch;

        // Create configurations
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<String, String>(props);
        this.consumer.subscribe(Collections.singleton(topic)); // just want to make sure it consumes only on topic
        // consumer.subscribe(Arrays.asList(topic)); the same
    }
    @Override
    public void run() {
        try {
            // Poll data
            while (true) {
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : records) {
                    this.logger.info("Key: " + record.key() + ", Value: " + record.value());
                    this.logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            this.logger.info("Received shutdown signal!");
        } finally {
            consumer.close(); // to avoid memory leak
            latch.countDown(); // to let main program know we are done with the consumer
        }
    }

    public void shutdown() {
        // wakeup - a special method to interrupt .poll()
        // It will throw an exception - WakeUpException
        // Stop working now! Wakeup!
        this.consumer.wakeup();
    }
}
