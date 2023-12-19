package org.java_kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();
        String topicName = "my_topic";
        int numOfMessages = 0;

        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seekToBeginning(Collections.singletonList(topicPartition));

        while (numOfMessages < 1000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            numOfMessages += records.count();
        }

        System.out.println("Number of messages read: " + numOfMessages);

        consumer.close();
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);

        return new KafkaConsumer<>(props);
    }
}