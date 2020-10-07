package com.samsoft.kafka.constant;

public class AppConfig {
    public static final String APPLICATION_ID = "HelloProducer2";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    public static final String TOPIC_NAME = "hello-producer-topic3";
    public static final int numEvents = 1000000;
    public final static String GROUP_ID = "PosValidatorGroup";
}
