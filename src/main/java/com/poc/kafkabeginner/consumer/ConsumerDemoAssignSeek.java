package com.poc.kafkabeginner.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerDemoAssignSeek {

    private static KafkaConsumer<String,String> consumer;

    public static void main(String[] args) {

        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";

        /**
         * Create Consumer Config
         */
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /**
         * Create Consumer
         */
        consumer = new KafkaConsumer<String, String>(properties);

        /**
         * Assign and Seek is mostly used to replay data or fetch specific message
         */

        //Assign
        TopicPartition topicPartition = new TopicPartition("first_topic", 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(topicPartition));

        //Seel
        consumer.seek(topicPartition, offsetToReadFrom);
        pollData();
    }

    /**
     * Poll for data
     */
    public static void pollData(){
        int noOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while(keepOnReading){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(3000));

            for(ConsumerRecord<String,String> record : consumerRecords) {
                numberOfMessagesReadSoFar++;
                log.info("Key: {} Value {} ", record.key(), record.value());
                log.info("Partition: {} Offset {} ", record.partition(), record.offset());
                if(numberOfMessagesReadSoFar >= noOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
            log.info("Exiting the application");
        }
    }
}
