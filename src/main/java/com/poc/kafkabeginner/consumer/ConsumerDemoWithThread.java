package com.poc.kafkabeginner.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

@Slf4j
public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public void run() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        CountDownLatch latch = new CountDownLatch(1);

        //Create consumer runnable
        log.info("Creating the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(
                bootstrapServer, groupId, latch);

        //Start the thread
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught Shutdown Hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.info("Application got interrupted {}", e);
        } finally {
            log.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger log = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServer,
                                String groupId, CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            /**
             * Create Consumer Config
             */
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            /**
             * Create Consumer
             */
            consumer = new KafkaConsumer<>(properties);

            /**
             * Subscribe to topic
             */
            consumer.subscribe(Collections.singleton("first_topic"));

            pollData();
        }

        /**
         * Poll for data
         */
        public void pollData() {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(3000));
                consumerRecords.forEach(record -> {
                    log.info("Key: {} Value {} ", record.key(), record.value());
                    log.info("Partition: {} Offset {} ", record.partition(), record.offset());
                });

            }
        }

        public void shutdown() {
            /**
             * the wakeup() is a special method to interrupt consumer.poll()
             * it will throw exception WakeUpException
             */
            consumer.wakeup();
        }

        @Override
        public void run() {
            try {
                pollData();
            } catch (WakeupException e) {
                log.info("Received shutdown signal!");
            } finally {
                consumer.close();
                /**
                 * Tell our main code that we're done with consumer
                 */
                latch.countDown();
            }
        }
    }
}