package com.github.fiona.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
        final String bootstrapServer = "localhost:9092";
        // create Producer properties
        Properties props = new Properties();

        // TODO - In the documentation https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
        // it use put() to set properties which is discouraged based on oracle java doc https://docs.oracle.com/javase/7/docs/api/java/util/Properties.html
        // Find kafka documentation see if can be corrected
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer <String, String> producer = new KafkaProducer<>(props);

        // create record
        for (int i = 0; i < 9; i++) {
            String topic = "first_topic";
            String value = "today is 4/15 " + i;
            String key = "id_" + i; // same key goes to the same partition

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // send data - async
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Execute every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received new Metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        // producer.flush();

        // flush & close
        producer.close();
    }
}
