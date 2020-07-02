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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
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

        KafkaConsumer<String, String> kafkaConsumer =
                new KafkaConsumer<>(properties);
        return kafkaConsumer;
    }

    //TODO Add the logic to extractId from Tweet
    private static String extractIdFromTweet(String value) {
    }


    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(KafkaElasticsearchConsumer.class.getName());

        logger.info("Creating Elasticsearch high level client");
        RestHighLevelClient esClient = restHighLevelClient();

        logger.info("Creating kafka consumer and subscribe to the topic");
        String groupId = "twitter-consumer-app";
        String topic = "twitter-topic";
        KafkaConsumer kafkaConsumer = kafkaConsumer(groupId);
        kafkaConsumer.subscribe(Collections.singleton(topic));

        while(true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            Integer recordsCount = records.count();
            logger.info("Received " + recordsCount + " records");

            //TODO do we need to create a new bulk request each time
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records){
                try {
                    String id = extractIdFromTweet(record.value());
                    //name of the index to send bulk request to
                    IndexRequest indexRequest = new IndexRequest("tweets")
                            .source(record.value(), XContentType.JSON)
                            .id(id);
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e){
                    logger.warn("skipping bad data: " + record.value());
                }
            }

            //TODO should we check if records count is 0 much earlier
            if (recordsCount > 0){
                try {
                    BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Committing the offset");
                    //TODO we should not commit until the bulk indexing has successfully finished
                    //all the way to shard replication in Elasticsearch
                    kafkaConsumer.commitSync();
                    logger.info("Offsets have been committed");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    //TODO Close the client gracefully

    }
}

