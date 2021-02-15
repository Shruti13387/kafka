package com.elasticsearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static JsonParser jsonParser = new JsonParser();

    public static RestHighLevelClient createClient() {
        String hostName = "kafka-project-for-5650770719.ap-southeast-2.bonsaisearch.net";
        String userName = "ua2qgsuqfv";
        String password = "mq8n3efr4t";

        //Don't do if running ES locally
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(h -> h.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }

    public static void main(String[] args) throws IOException {
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        pollData(consumer);
        // client.close();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        /**
         * Create Consumer Config
         */
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        /**
         * Create Consumer
         */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        /**
         * Subscribe to topic
         */
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    /**
     * Poll for data
     */
    public static void pollData(KafkaConsumer<String, String> consumer) throws IOException {
        RestHighLevelClient client = createClient();

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            BulkRequest bulkRequest = new BulkRequest();
            int recordCounts = consumerRecords.count();
            System.out.println("Received " + recordCounts + " records");
            consumerRecords.forEach(record -> {
//                 String id = record.topic() + "_" +record.partition() + "_" +record.offset();  //If unique Id is not present
                String id = null;
                try{
                    id = extractIdFormTweets(record.value());
                }catch (NullPointerException e){
                    System.out.println("Skipping bad data exception");
                }

                IndexRequest indexRequest = new IndexRequest("twitter")
                        .type("tweets")
                        .id(id) //To make ID Idempotent
                        .source(record.value(), XContentType.JSON);

                bulkRequest.add(indexRequest);
            });
            if (recordCounts>0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                System.out.println("Committing Offsets..");
                consumer.commitSync();
                System.out.println("Offsets have been committed..");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String extractIdFormTweets(String tweetJson) {
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
