package com.poc.twitter.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TwitterProducer {


    List<String> terms = Lists.newArrayList("kafka", "bitcoin", "India");
    private String consumerKey = "5C9bKLAQTNH6ChnMLWcJrn7oO";
    private String consumerSecret = "dOCIl3DEAsxdnThSdvjVxnDGpjNgTzxHC764O5BjV6J96cSUT2";
    private String token = "1359541759370350595-X0PJ2GDc8QgKFgCYCttJIl11b3wbwM";
    private String secret = "W9mzSbs90lOxcTnjNfzqctkG7yDNwrGOL5P8vILA5MmTo";

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    /**
     * 1. Create Twitter client
     * 2. Create Kafka Producer
     * 3. Loop the send tweet to Kafka
     */
    private void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        /**
         * 1. Create Twitter Client
         */
        Client client = createTwitterClient(msgQueue);
        client.connect();

        /**
         * 2. Create Kafka Producer
         */
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        /**
         * Add Shutdown Hook
         */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping application...");
            log.info("Shutting down client from twitter... ");
            client.stop();
            log.info("closing producer...");
            kafkaProducer.close();
            log.info("done!");
        }));

        /**
         * Now, msgQueue and eventQueue will now start being filled with messages/events. Read from these queues however you like.
         */
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                log.info(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), (metadata, e) -> {
                    if (e != null) {
                        log.error("Something bad happened", e);
                    }
                });
            }
        }
        log.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        //  hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {

        Properties properties = new Properties();
        String bootStrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);

        /**
         * Key and value serializer help producer know what kind of values we are sending to kafka
         * and how these values will be serialized to bytes
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * High Throughput producer (at the expence of a bit latency and CPU Usage)
         */
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        /**
         * Create safe Producer
         */
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        return new KafkaProducer<String, String>(properties);
    }
}
