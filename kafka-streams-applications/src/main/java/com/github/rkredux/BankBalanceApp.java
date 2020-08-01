package com.github.rkredux;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class BankBalanceApp {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String customerTransactionsTopic = "transactions-topic";
        String aggregatedBankBalanceTopic = "customer-balance-topic";

        String[] users = new String[] {"Rahul", "Meghan", "Alivia", "Priyanka", "Ravdip", "Angad"};
        Integer countOfTransactions = 10;

        BankTransactionsProducer bankTransactionsProducer = new BankTransactionsProducer(
                bootstrapServers,
                customerTransactionsTopic,
                users,
                countOfTransactions
        );
        System.out.println("Starting generating bank transactions");
        bankTransactionsProducer.start();

        BankBalanceAggregator bankBalanceAggregator = new BankBalanceAggregator(
                customerTransactionsTopic,
                aggregatedBankBalanceTopic
        );
        System.out.println("Starting bank balance aggregation stream processing");
        bankBalanceAggregator.start();
    }

    private static class BankTransactionsProducer extends Thread{
        final Logger logger = LoggerFactory.getLogger(BankTransactionsProducer.class);
        private final String bootstrapServers;
        private final String topicName;
        private final Integer countOfTransactions;
        private final String[] users;

        private BankTransactionsProducer(String bootstrapServers, String topicName, String[] users, Integer countOfTransactions) {
            this.bootstrapServers = bootstrapServers;
            this.topicName = topicName;
            this.users = users; 
            this.countOfTransactions = countOfTransactions;
        }

        @Override
        public void run() {
            super.run();
            //producer configs
            String bootstrapServers = "127.0.0.1:9092";
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty("acks", "all");
            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            //generate bank users
            JSONArray bankCustomers = generateCustomerArray(users);

            for (int transactionCount=0; transactionCount < countOfTransactions; transactionCount++){

                try {
                    Thread.currentThread().sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                Integer customerIndex = transactionCount % bankCustomers.length();
                String customerName = null;
                String customerId = null;
                try {
                    customerName = (String) bankCustomers.getJSONObject(customerIndex).get("customerName");
                    customerId = (String) bankCustomers.getJSONObject(customerIndex).get("customerId");
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                String transactionId = UUID.randomUUID().toString();
                String value = generateTransactionRecord(customerName, customerId);

                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,transactionId,value);
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
                });
            }
            producer.flush();
            producer.close();
        }

        private static JSONArray generateCustomerArray(String[] listOfUsers) {
            JSONArray customerArray = new JSONArray();
            for (int i=0; i< listOfUsers.length; i++) {
                JSONObject customerObject = new JSONObject();
                try {
                    customerObject.put("customerId", UUID.randomUUID().toString());
                    customerObject.put("customerName", listOfUsers[i]);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                customerArray.put(customerObject);
            }
            return customerArray; 
        }

        private static String generateTransactionRecord(String userName, String userId) {
            //transaction timestamp
            SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
            Date date = new Date(System.currentTimeMillis());
            //transaction amount
            Random r = new Random();
            int amount = r.nextInt(100);

            JSONObject transactionRecord = new JSONObject();
            try {
                transactionRecord.put("customerId",userId);
                transactionRecord.put("customerName", userName);
                transactionRecord.put("transactionTimestamp", formatter.format(date));
                transactionRecord.put("amount", amount);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            return transactionRecord.toString();
        }

    }


    private static class BankBalanceAggregator extends Thread {

        private final String aggregatedBankBalanceTopic;
        private final String customerTransactionsTopic;

        public BankBalanceAggregator(String customerTransactionsTopic, String aggregatedBankBalanceTopic) {
            this.aggregatedBankBalanceTopic = aggregatedBankBalanceTopic;
            this.customerTransactionsTopic = customerTransactionsTopic; 
        }

        @Override
        public void run() {
            super.run();
            //streams config
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

            //JSON Serde set up
            final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
            final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
            final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

            //building streams topology
            StreamsBuilder builder = new StreamsBuilder();

            KStream<String, JsonNode> customerTransactions = builder.stream(customerTransactionsTopic,
                    Consumed.with(Serdes.String(), jsonSerde));

            //creating initial bank balance object
            ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
            initialBalance.put("count", 0);
            initialBalance.put("balance", 0);
            initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

            KTable<String, JsonNode> bankBalance = customerTransactions
                    .groupByKey()
                    .aggregate(() -> initialBalance,
                            (key,transaction,balance) -> newBalance(transaction,balance),
                            Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                            .withKeySerde(Serdes.String())
                            .withValueSerde(jsonSerde)
                    );

            bankBalance.toStream().to(aggregatedBankBalanceTopic, Produced.with(Serdes.String(), jsonSerde));

            KafkaStreams streamsApp = new KafkaStreams(builder.build(), props);
            //only in dev
            streamsApp.cleanUp();
            streamsApp.start();
            System.out.println(streamsApp.toString());
            Runtime.getRuntime().addShutdownHook(new Thread(streamsApp::close));
        }

        //function to aggregate the records
        private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
            ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
            newBalance.put("count", balance.get("count").asInt() + 1);
            newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());
            Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
            Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
            Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
            newBalance.put("time", newBalanceInstant.toString());
            return newBalance;
        }

    }

}