package com.github.fiona.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
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
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello Kafka!");

        // send data - async
        producer.send(record);

        // producer.flush();

        // flush & close
        producer.close();
    }
}
