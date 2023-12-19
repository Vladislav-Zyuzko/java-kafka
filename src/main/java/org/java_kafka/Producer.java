package org.java_kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;

public class Producer {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = createProducer();
        String topicName = "my_topic";
        int numOfMessages = 1000;

        for (int i = 0; i < numOfMessages; i++) {
            String message = LocalDateTime.now().toString();
            producer.send(new ProducerRecord<>(topicName, message));
        }

        producer.close();
    }

    private static KafkaProducer<String, String> createProducer() {
        java.util.Properties props = new java.util.Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }
}