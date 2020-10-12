package com.samsoft.kafka.producer.stringmsg;

import com.samsoft.kafka.constant.AppConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.prefs.AbstractPreferences;

public class ProducerSendStringKeyValue {
    private static Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        logger.info("Creating properties file with given configuration");
        Properties prop = new Properties();
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.APPLICATION_ID);
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);

        logger.info("Creating Kafka producer with the properties file");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        logger.info("Start sending messages to topic  : "+AppConfig.TOPIC_NAME);

        for (int i = 0; i < AppConfig.numEvents; i++) {
            producer.send(new ProducerRecord<>(AppConfig.TOPIC_NAME, "Key"+i, "Multiple new messages : "+i));
        }

        logger.info("Finished sending messages to topic : " + AppConfig.TOPIC_NAME);
    }
}
