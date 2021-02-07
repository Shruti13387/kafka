package com.poc.kafkabeginner.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

@Slf4j
public class ProducerDemoWithCallback {

    Properties properties;
    KafkaProducer<String, String> kafkaProducer;

    public ProducerDemoWithCallback() {
        properties = new Properties();
        String bootStrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);

        /**
         * Key and value serializer help producer know what kind of values we are sending to kafka
         * and how these values will be serialized to bytes
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        createProducer();
    }

    private void createProducer() {

        /**
         * Create Producer
         */
        kafkaProducer = new KafkaProducer<String, String>(properties);

    }

    public void sendData() {

        IntStream.range(0, 10).forEach(i -> {
            /**
             * Producer Record
             */
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world Java" + i);

            /**
             * Send Data - sync
             */
            kafkaProducer.send(record, (recordMetadata, exception) -> {
                if(exception == null){
                    log.info("Received new Metadata. \n Topics: {} \n Partition: {} \n Offset: {} \n Timestamp: {}",
                            recordMetadata.topic(),recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                }else{
                    log.error("Error While Producing {}", exception);
                }
            });
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
