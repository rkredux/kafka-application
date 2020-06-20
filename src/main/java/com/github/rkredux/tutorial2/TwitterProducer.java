package com.github.rkredux.tutorial2;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer(){
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){

        logger.info("Setting up the client");

        /** Set up kafka producer */
        String topic = "twitter_topic";
        String bootstrapServers = "127.0.0.1:9092";
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer(bootstrapServers);

        /** Set up your blocking queues and twitter client: */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);
        //this initiates pulling the tweets and then writing to the local queue
        client.connect();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Shutting down the Twitter client");
            client.stop();
            logger.info("Shutting down the Kafka Producer");
            kafkaProducer.close();
            logger.info("Done!");
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }
            //TODO producer message compression
            //TODO Batch sizing
            //TODO setting up key using the user who tweeted so the
            //TODO same users tweets goes to the same partition
            //TODO setting up a pace to securely pass the secrets
            //TODO Test the whole application using console consumer

            if (msg != null){
                logger.info(msg);
                String hash = generateHashFromUser(msg.toString());
                String key = "id_" + hash;

                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,msg);
                kafkaProducer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        logger.info("Received new metadata \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition:" + recordMetadata.partition() + "\n" +
                                    "Offset:" + recordMetadata.offset() + "\n" +
                                    "Timestamp:" + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing:" + e);
                    }
                });
            }
        }
    }

    private String generateHashFromUser(String user) {
        String userHash = Hashing.sha256()
                .hashString(user, StandardCharsets.UTF_8)
                .toString();
        return userHash;
    }

    //create kafka producer
    public KafkaProducer<String, String> createKafkaProducer(String bootstrapServers){

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //settings for a safe idempotent producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        //high throughput settings
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    //create twitter client
    public Client createTwitterClient(BlockingQueue<String> queue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        ArrayList<String> terms = Lists.newArrayList("kafka", "java");
        hosebirdEndpoint.trackTerms(terms);
        //TO DO create a secrets passing mechanism so we don't commit in the code
        Authentication hosebirdAuth = new OAuth1("consumerKey", "consumerSecret", "token", "secret");

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(queue));

        Client hosebirdClient = builder.build();
        //Attempts to establish a connection.
        return hosebirdClient;
    }


}
