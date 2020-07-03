package rkredux.tutorial3;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class KafkaElasticsearchConsumer{


    public static RestHighLevelClient restHighLevelClient(){

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http"),
                        new HttpHost("localhost", 9202, "http" )));

        return restHighLevelClient;
    }


    public static KafkaConsumer kafkaConsumer(String groupId){

        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer =
                new KafkaConsumer<>(properties);
        return kafkaConsumer;
    }


    private static String extractIdFromTweet(String tweetValue) throws JSONException {
        return (String) new JSONObject(tweetValue).get("id_str");
    }


    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(KafkaElasticsearchConsumer.class.getName());

        logger.info("Creating Elasticsearch high level client");
        RestHighLevelClient esClient = restHighLevelClient();

        logger.info("Creating kafka consumer and subscribing it to the topic");
        String groupId = "twitter-consumer-app";
        String topic = "twitter-topic";
        KafkaConsumer kafkaConsumer = kafkaConsumer(groupId);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        String index = "twitter-tweets";

        while(true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            Integer recordsCount = records.count();

            if ( recordsCount > 0) {
                logger.info("Received " + recordsCount + " Records");
                BulkRequest bulkRequest = new BulkRequest()
                        .timeout(TimeValue.timeValueMinutes(2))
                        .setRefreshPolicy("wait_for")
                        .waitForActiveShards(ActiveShardCount.ALL);

                for (ConsumerRecord<String,String> record : records){
                    try {
                        String id = extractIdFromTweet(record.value());
                        IndexRequest indexRequest = new IndexRequest(index)
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        bulkRequest.add(indexRequest);
                    } catch (NullPointerException | JSONException e) {
                        logger.warn("Bad data received from Twitter, skipping it");
                    }
                }

                try {
                    BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    if (!bulkResponse.hasFailures()){
                        kafkaConsumer.commitSync();
                    } else {
                        for (BulkItemResponse bulkItemResponse : bulkResponse) {
                            if (bulkItemResponse.isFailed()) {
                                //log and ship to a DLQ
                                logger.warn("Indexing operation failed with reason: "
                                        + bulkItemResponse.getFailureMessage());
                                //TODO implement a DLQ in Java
                                //insertToDeadLetterQueue(bulkItemResponse.);
                            }
                        }
                    }
                } catch (IOException e) {
                    logger.warn("Encountered an exception");
                    e.printStackTrace();
                }
            } else {
                logger.info("Received " + recordsCount + " Records, polling again");
            }

        }
    }
}

