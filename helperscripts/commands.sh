#standard terminal commands to launch new topics, console producer and consumer to test the streams application
#create a new topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic insertNameOfTopic

#create log compacted topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic insertNameOfTopic --config cleanup.policy=compact

#launch a Kafka console consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic insertNameOfTopic \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

#produce data to the application
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic insertNameOfTopic
#enter data to the producer as required

#list all the topics in Kafka
bin/kafka-topics.sh --list --zookeeper localhost:2181
