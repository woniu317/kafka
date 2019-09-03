package com.woniu.kafka.consumer;

import com.woniu.kafka.consumer.rebalance.Rebalance;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class Consumer {

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("start exit...");
                //等待主线程退出
                StopConsumer.stopConsumer.set(true);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        Consumer consumer = new Consumer();
        consumer.subscribeModule();
//        consumer.subPartition(new Integer[]{0,1,2});
        System.out.println(consumer.getAllPartition("zhoushijie"));
    }

    public Properties buildProperties(String groupId, String brokers) {
        //初始化kafka
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("max.poll.records", 10);
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    public void subscribeModule() {
        Properties properties = buildProperties("client_2", "localhost:9092");
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("zhoushijie"), new Rebalance(kafkaConsumer));
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        try {
            Duration duration = Duration.ofSeconds(100);
            while (true) {
                System.out.println("continue consumer>>>>>>>>>>>>>>>");
                ConsumerRecords<String,String> records = kafkaConsumer.poll(duration);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        if (StopConsumer.stopConsumer.get()) {
                            System.out.println("commit currentOffsets");
                            kafkaConsumer.commitSync(currentOffsets);
                            return ;
                        }
                        System.out.println("process data:"+record.partition()+"_"+record.offset());
                        currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1, "no metadata"));
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                kafkaConsumer.commitAsync();
            }
        } finally {
            kafkaConsumer.close();
        }
    }

    public void subPartition(Integer[] kafkaPartition) {
        Properties properties = buildProperties("1", "localhost:9092");
        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(properties);
        List<TopicPartition> partitionList = new ArrayList<>();
        String topic = "zhoushijie";
        for (Integer k : kafkaPartition) {
            partitionList.add(new TopicPartition(topic, k));
        }
        kafkaConsumer.assign(partitionList);
        while (true) {
            Duration duration = Duration.ofSeconds(100);
            ConsumerRecords records = kafkaConsumer.poll(duration);
            records.forEach(x -> System.out.println(x));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public List<PartitionInfo> getAllPartition(String topic) {
        Properties properties = buildProperties("client_2", "localhost:9092");
        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(properties);
        List<PartitionInfo> list = kafkaConsumer.partitionsFor(topic);
        list.forEach(x -> System.out.println(x.partition()));
        return list;
    }


}
