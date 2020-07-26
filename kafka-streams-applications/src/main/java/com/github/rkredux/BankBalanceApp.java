package com.github.rkredux;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.sql.Timestamp;

public class BankBalanceApp {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String topicName = "customer-transactions-topic";
        Integer countOfTransactions = 10;
        BankTransactionsProducer bankTransactionsProducer = new BankTransactionsProducer(bootstrapServers, topicName, countOfTransactions);
        bankTransactionsProducer.start();
    }

    private static class BankTransactionsProducer extends Thread{
        final Logger logger = LoggerFactory.getLogger(BankTransactionsProducer.class);
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
                Integer customerIndex = transactionCount % users.length;
                Date date = new Date();
                String key = bankCustomers[customerIndex].getCustomerId();
                String value = bankCustomers[customerIndex].getCustomerName() + ","
                        + transactionCount + ","
                        + new Timestamp(date.getTime());

                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,key, value);
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
    }

    private static class BankCustomer{
        private final String userName;
        private final String customerId; 

        private BankCustomer(String user) {
             this.userName = user;
             this.customerId = UUID.randomUUID().toString();
        }
        private String getCustomerId(){
            return this.customerId;
        }
        private String getCustomerName(){
            return this.userName;
        }
    }


}