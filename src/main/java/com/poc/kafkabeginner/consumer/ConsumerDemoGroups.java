package com.poc.kafkabeginner.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerDemoGroups {

    private static KafkaConsumer<String,String> consumer;

    public static void main(String[] args) {

        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";

        /**
         * Create Consumer Config
         */
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /**
         * Create Consumer
         */
        consumer = new KafkaConsumer<String, String>(properties);

        /**
         * Subscribe to topic
         */
        consumer.subscribe(Collections.singleton("first_topic"));

        pollData();
    }

    /**
     * Poll for data
     */
    public static void pollData(){
        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(3000));

            consumerRecords.forEach(record -> {
                log.info("Key: {} Value {} ", record.key(), record.value());
                log.info("Partition: {} Offset {} ", record.partition(), record.offset());
            });

        }
    }
}
