package rkredux.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("acks", "all");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        long topicPrefix = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {

            String topicName = topicPrefix + "topic";
            logger.info("The topic name is: " + topicName);
            String key = "id_" + i;
            String value = "This is message: " + i;

            logger.info("Key:" + key);

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topicName,key,value);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("Error while producing:" + e);
                    }
                }
            }).get();
        };
        producer.flush();
        producer.close();
    }
}
