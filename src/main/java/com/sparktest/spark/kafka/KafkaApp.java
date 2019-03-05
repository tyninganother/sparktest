package com.sparktest.spark.kafka;

/**
 * test
 *
 * @author haining
 */
public class KafkaApp {

    public static void main(String[] args) {
        new KafkaProducer(KafkaPropertices.TOPIC).start();
    }

}
