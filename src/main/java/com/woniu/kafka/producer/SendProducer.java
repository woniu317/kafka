package com.woniu.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class SendProducer extends Thread {
            public static void main(String[] args) {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println("Starting exit...")));
                producerMsg();
            }

            private static void producerMsg() {
                Properties properties = buildProperties();
                Producer<String, String> producer = null;
        try {
            String topic = "zhoushijie";
            producer = new KafkaProducer<>(properties);
            for (int i = 0; i < 30; i++) {
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i)));
//                producer.send(new ProducerRecord<>(topic,  Integer.toString(i)));
                if (i % 10 == 0) {
                    Thread.sleep(100);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static Properties buildProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //使用自己的分区划分器
        properties.put("partitioner.class", "com.woniu.kafka.producer.MyPartitioner");
        return properties;
    }
}
