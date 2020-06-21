package rkredux.tutorial3;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaElasticsearchConsumer {

    public KafkaElasticsearchConsumer() {
    }

    public static void main(String[] args) {
        new KafkaElasticsearchConsumer().run();
    }

    private void run() {

        Logger logger = LoggerFactory.getLogger(KafkaElasticsearchConsumer.class.getName());

        //options for kafka consumer app
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-latest-application";
        String topic = "second_topic";

        //options for elasticsearch client


        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable and elasticsearch client
        logger.info("Creating the consumer thread and elasticsearch client");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }


    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private RestHighLevelClient esClient;

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            //create the elasticsearch client
            esClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9201, "http")));

            // kafka consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create kafka consumer
            consumer = new KafkaConsumer<String, String>(properties);
            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {
            // poll for new data
            logger.info("Starting to poll the topic");
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));
                    BulkRequest bulkRequest = new BulkRequest();
                    for (ConsumerRecord<String, String> record : records){
                    //    loop over the records to form the bulkRequest object
                    }
                    //commit the bulk request
                    //sync
                    //BulkResponse bulkResponse = esClient.bulk(request, RequestOptions.DEFAULT);
                    //async
                    //esClient.bulkAsync(request, RequestOptions.DEFAULT, listener);
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }

}