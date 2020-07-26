package com.github.rkredux;

import com.sun.jmx.remote.util.ClassLogger;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class BankBalanceApp {
    public static void main(String[] args) {
    //    Multi-threaded application
    //    streams app thread - consume, do aggregation and produce to output topic
    //    producer app thread - produce transactions and write to topic

        String bootstrapServers = "127.0.0.1:9092";
        String topicName = "customer-transactions-topic";
        Integer countOfTransactions = 100000;
        BankTransactionsProducer bankTransactionsProducer = new BankTransactionsProducer(bootstrapServers, topicName, countOfTransactions);
        bankTransactionsProducer.start();
    }

    private static class BankTransactionsProducer extends Thread{
        private final String bootstrapServers;
        private final String topicName;
        private final Integer countOfTransactions;

        private BankTransactionsProducer(String bootstrapServers, String topicName, Integer countOfTransactions) {
            this.bootstrapServers = bootstrapServers;
            this.topicName = topicName;
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
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            //bank users
            String[] users = new String[] {"Rahul", "Meghan", "Alivia", "Priyanka", "Ravdip", "Angad"};
            BankCustomer[] bankCustomers = new BankCustomer[users.length];
            for (int i=0; i< users.length; i++) {
                bankCustomers[i] = new BankCustomer(users[i]);
            }

            for (int transactionCount=0; transactionCount < countOfTransactions; transactionCount++){
                //TODO implement something to pick the right customer here
                String key = null;
                Object value = null;
                ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(topicName,key, value);
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
                //TODO what does this get do.
                }).get();
            }

        }
    }

    private static class BankCustomer{
        private final String userName;
        private final String customerId;

        public BankCustomer(String user) {
             this.userName = user;
             this.customerId = UUID.randomUUID().toString();
        }
    }

}