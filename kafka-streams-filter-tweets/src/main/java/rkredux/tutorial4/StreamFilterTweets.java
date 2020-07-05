package rkredux.tutorial4;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;


public class StreamFilterTweets {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "high-follower-tweets");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //build topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter-topic");
        KStream<String, String> filteredTopic = inputTopic.filter(
                (k,jsonTweet) -> extractUserFollowersCount(jsonTweet) > 10000
        );
        filteredTopic.to("tweets_from_users_with_high_follower_count");

        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                props
        );

        //Start the app
        kafkaStreams.start();

    }

    private static Integer extractUserFollowersCount(String tweetJson){
        try {
            return JsonParser.parseString(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (JsonSyntaxException e) {
            e.printStackTrace();
            return 0;
        }
    }

}
