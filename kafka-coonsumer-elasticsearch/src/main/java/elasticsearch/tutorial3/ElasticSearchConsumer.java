package elasticsearch.tutorial3;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient(){

        //////////////////////////
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //////////////////////////

          String hostname = "localhost";
          RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


        //////////////////////////
        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        //////////////////////////
//
//        // replace with your own credentials
//        String hostname = ""; // localhost or bonsai url
//        String username = ""; // needed only for bonsai
//        String password = ""; // needed only for bonsai
//
//        // credentials provider help supply username and password
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY,
//                new UsernamePasswordCredentials(username, password));
//
//        RestClientBuilder builder = RestClient.builder(
//                new HttpHost(hostname, 443, "https"))
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    @Override
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String,String> createConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "Kafka-demo-elasticsearch";
        // String topic = "first_topic";
        //consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        // create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

        return consumer;






    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();


        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");
        //poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();
            logger.info("Received " + recordCount +" records");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record: records) {
               String jsonString = record.value(); //getting tweets from consumer
                //2 strats to make consumer idempotent
                //1:  kafka genereic ID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                //2: Twitter feeed specific id:
                try {

                    String id = extraxtIdFromTweet(record.value());


                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id //to make consumer idempotent



                    ).source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest); //add to bulk request
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data" + record.value());
                }

            }
            if (recordCount > 0 ) {
                BulkResponse bulkItemResponses =  client.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }



        //close gracefully
       // client.close();
    }
}
private static JsonParser jsonParser = new JsonParser();
private static String extraxtIdFromTweet(String tweetJson) {

// gson library

    return jsonParser.parse(tweetJson)
            .getAsJsonObject()
            .get("id_str")
            .getAsString();



}









}
