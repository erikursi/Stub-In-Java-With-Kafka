package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user-transformer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");



        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer  = new KafkaConsumer<>(props);KafkaProducerApp producer = new KafkaProducerApp()) {
            consumer.subscribe(Collections.singletonList("input-topic"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() == null || record.value().isBlank()) {
                        System.err.println("Skipping empty or blank message.");
                        continue;
                    }
                    try {
                        System.err.println("Reading user");
                        // Parse JSON and transform
                        User user = mapper.readValue(record.value(), User.class);
                        if (user.getLogin() == null || user.getPassword() == null) {
                            System.err.println("Invalid user data: " + record.value());
                            producer.sendMessage("error-topic", record.value());
                            continue;
                        }
                        user.setStatus("Processed");

                        // Send to output-topic
                        String transformedMessage = mapper.writeValueAsString(user);
                        producer.sendMessage("output-topic", transformedMessage);

                    } catch (Exception e) {
                        System.err.println("Failed to process record: " + record.value() + " due to: " + e.getMessage());
                        producer.sendMessage("error-topic", record.value());
                    }
                }
            }
        }

    }

}
