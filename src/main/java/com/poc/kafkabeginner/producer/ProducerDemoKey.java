package com.poc.kafkabeginner.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

@Slf4j
public class ProducerDemoKey {

    private static KafkaProducer<String, String> kafkaProducer;

    public static void main(String[] args) {
        Properties properties;;

        properties = new Properties();
        String bootStrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);

        /**
         * Key and value serializer help producer know what kind of values we are sending to kafka
         * and how these values will be serialized to bytes
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * Create Producer
         */
        kafkaProducer = new KafkaProducer<>(properties);

        sendData();

    }

    public static void sendData() {

        IntStream.range(0, 10).forEach(i -> {
            String topic = "first_topic";
            String value =  "hello world Java" + i;
            String key =  "Id_" + i;

            /**
             * Producer Record
             */
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            log.info("Key: {}", key);
            /**
             * Partition 0: 3, 6, 9
             * Partition 1: 1, 4, 8
             * Partition 2: 0, 2, 5, 7
             */
            /**
             * Send Data - sync
             */
            try {
                kafkaProducer.send(record, (recordMetadata, exception) -> {
                    if (exception == null) {
                        log.info("Received new Metadata. \n Topics: {} \n Partition: {} \n Offset: {} \n Timestamp: {}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        log.error("Error While Producing {}", exception);
                    }
                }).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
