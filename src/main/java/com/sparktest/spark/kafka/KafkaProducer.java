package com.sparktest.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * message producer
 *
 * @author haining
 */
public class KafkaProducer extends Thread {
    private String topic;
    private Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put("metadata.broker.list", KafkaPropertices.BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        /*
         * The number of acknowledgments the producer requires the leader to have received before considering a request complete.
         * This controls the durability of the messages sent by the producer.
         *
         * request.required.acks = 0 - means the producer will not wait for any acknowledgement from the leader.
         *  不会等待leader（握手机制）
         * request.required.acks = 1 - means the leader will write the message to its local log and immediately acknowledge
         *  写入本地日志里，不会等待所有副本，只会等待leader
         * request.required.acks = -1 - means the leader will wait for acknowledgement from all in-sync replicas before acknowledging the write
         *  等待所有副本都过来之后才返回
         */
        properties.put("request.required.acks", "1");
        this.producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String message = "message_" + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Send:" + message);
            messageNo ++;

            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
