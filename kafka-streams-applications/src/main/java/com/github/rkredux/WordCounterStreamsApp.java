package com.github.rkredux;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class WordCounterStreamsApp {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-counter-app" );
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String.valueOf(Serdes.String().getClass()));
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String.valueOf(Serdes.String().getClass()));

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Long> stream = streamsBuilder.stream("word-stream");
        final KStream<String, Long> filter = stream.filter((key, value) -> value > 0);

        //    build the topology using Streams Builder
    //    source processor
    //    sink processor
    //    node

    }
}
