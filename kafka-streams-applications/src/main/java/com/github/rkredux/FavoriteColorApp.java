package com.github.rkredux;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {

    public static void main(String[] args) {
        String userColorInputTopic = "user-color-topic";
        String favoriteColorOutputTopic = "favorite-color-count-topic";

        //TODO -
        // topic helper class - create topic if does not exist

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputUserColorStream = builder.stream(userColorInputTopic);
        KTable<String, Long> colorCountTable = inputUserColorStream
                .filter((user,color) -> Arrays.asList("red","blue","green").contains(color))
                .toTable()
                .toStream()
                .selectKey((user,color) -> color)
                .groupByKey()
                .count(Named.as("colorCounts"));
        colorCountTable.toStream().to(favoriteColorOutputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streamsApp = new KafkaStreams(builder.build(), props);
        streamsApp.start();
        System.out.println(streamsApp.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streamsApp.close()));

    }
}
