package com.github.rkredux;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {

    public static void main(String[] args) {

        String userInputTopic = "user-input-topic";
        String userFavoriteColorTopic = "userFavoriteColorTopic";
        String favoriteColorCountTopic = "favorite-color-count-topic";

        //TODO - create the missing topics programmatically here

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String>  userInput = builder.stream(userInputTopic);
        //cleaning
        userInput
                .filter((key,value) -> value.contains(","))
                .selectKey((key,value) -> value.split(",")[0])
                .mapValues((key,value) -> value.split(",")[1])
                .mapValues((key,value) -> value.toLowerCase())
                .filter((user,color) -> Arrays.asList("red", "blue", "green").contains(color))
                .to(userFavoriteColorTopic);

        KTable<String, String> usersAndColorsCountTable = builder.table(userFavoriteColorTopic);
        //aggregation
        KTable <String, Long> colorCountTable = usersAndColorsCountTable
                .groupBy((user,color) -> new KeyValue<>(color, color))
                .count(Named.as("CountsByColors"));

        colorCountTable
                .toStream()
                .to(favoriteColorCountTopic);

        KafkaStreams streamsApp = new KafkaStreams(builder.build(), props);
        streamsApp.cleanUp();
        streamsApp.start();

        System.out.println(streamsApp.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streamsApp::close));
    }
}
