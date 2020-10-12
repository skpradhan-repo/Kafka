package com.samsoft.kafka.consumer;


import com.samsoft.kafka.constant.AppConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class HelloConsumer {
    private static Logger logger = LogManager.getLogger();
    public static void main(String[] args) {
        logger.info("Creating Properties ");
        logger.info("Loading kafka server details to properties file");
        InputStream ios = null;
        Properties consumerProperties = new Properties();
        try {
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, AppConfig.APPLICATION_ID);
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.GROUP_ID);
        } catch (Exception e) {
            logger.error("Error while loading the kafka properties file "+e.getMessage());
        }
        logger.info("Crating Kafka Consumer from topic : "+AppConfig.TOPIC_NAME);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
        consumer.subscribe(Arrays.asList(AppConfig.TOPIC_NAME));
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info(record.value());
            }
        }
    }
}
