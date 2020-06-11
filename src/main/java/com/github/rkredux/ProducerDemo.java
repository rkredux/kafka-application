package com.github.rkredux;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("acks", "all");

//        create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

//        create a producer record

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic","hello world");

//        send data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

            }
        })
        producer.flush();
        producer.close();
    }
}
