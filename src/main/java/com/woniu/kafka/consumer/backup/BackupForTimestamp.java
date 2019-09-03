package com.woniu.kafka.consumer.backup;

import com.google.common.collect.Sets;
import com.woniu.kafka.consumer.ConsumerInfo;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 从指定时刻从新消费kafka消息
 */
public class BackupForTimestamp {

    public static void main(String[] args) throws ParseException {
        String bootstrapServers = "localhost:9092";
        String topic = "zhoushijie";
        String groupId = "client4";
        String startTime = "20190903102000";
        String endTime = "20190903112000";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        long st = sdf.parse(startTime).getTime();
        long et = sdf.parse(endTime).getTime();
        BackupForTimestamp backupForTimestamp = new BackupForTimestamp();
        ConsumerInfo consumerInfo = new ConsumerInfo(bootstrapServers, topic, groupId);
        consumerInfo.setStartTime(st);
        consumerInfo.setEndTime(et);
//        backupForTimestamp.consumerAfterStartTime(consumerInfo);
        backupForTimestamp.consumerBetweenTime(consumerInfo);
    }

    public Stream<PartitionInfo> getPartitions(ConsumerInfo consumerInfo) {
        Properties properties = buildProperties(consumerInfo.getGroupId(), consumerInfo.getBrokers());
        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(properties);
        List<PartitionInfo> list = kafkaConsumer.partitionsFor(consumerInfo.getTopicId());
        return list.stream();
    }

    /**
     * 初始化kafka
     *
     * @param groupId
     * @param brokers
     * @return
     */
    public Properties buildProperties(String groupId, String brokers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    /**
     * 消费topic从startTime到endTime的所有信息
     *
     * @return
     */
    public Map<TopicPartition, Long> consumerAfterStartTime(ConsumerInfo consumerInfo) {
        Properties props = buildProperties(consumerInfo.getGroupId(), consumerInfo.getBrokers());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> topicPartitionList = getPartitions(consumerInfo)
                .map(x -> new TopicPartition(consumerInfo.getTopicId(), x.partition())).collect(Collectors.toList());
        Map<TopicPartition, Long> partitionStartOffset = getGreaterOffset(consumer, topicPartitionList, consumerInfo, consumerInfo.getStartTime());
        consumer.assign(topicPartitionList);
        partitionStartOffset.entrySet().stream().forEach(x -> {
            consumer.seek(x.getKey(), x.getValue());
        });
        Duration duration = Duration.ofMillis(1000);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(duration);
                records.forEach(x -> System.out.println(x.offset() + "_" + x.partition() + "_" + x.value()));
//            consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 消费topic从startTime到endTime的所有信息
     *
     * @return
     */
    public void consumerBetweenTime(ConsumerInfo consumerInfo) {
        Properties props = buildProperties(consumerInfo.getGroupId(), consumerInfo.getBrokers());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> topicPartitionList = getPartitions(consumerInfo)
                .map(x -> new TopicPartition(consumerInfo.getTopicId(), x.partition())).collect(Collectors.toList());
        Map<TopicPartition, Long> partitionStartOffset = getGreaterOffset(consumer, topicPartitionList, consumerInfo, consumerInfo.getStartTime());
        consumer.assign(topicPartitionList);
        partitionStartOffset.entrySet().stream().forEach(x -> {
            consumer.seek(x.getKey(), x.getValue());
        });
        Duration duration = Duration.ofMillis(1000);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(duration);
                if (records == null || records.isEmpty()) {
                    return;
                }
                Set<TopicPartition> assignment = consumer.assignment();
                Set<TopicPartition> removePartitions = Sets.newHashSet();
                records.forEach(x -> {
//                System.out.println("process " + x.partition() + "_" + x.offset() + "_" + x.value()+"_"+x.timestamp()+"_"+consumerInfo.getEndTime());
                    //若消息不在运行范围内，则无需处理
                    if (x.timestamp() > consumerInfo.getEndTime()) {
                        Set<TopicPartition> tmpTopicPartition = assignment.stream().filter(y -> y.partition() == x.partition()).collect(Collectors.toSet());
                        removePartitions.addAll(tmpTopicPartition);
                        return;
                    }
                    //处理消息
                    System.out.println("process " + x.partition() + "_" + x.offset() + "_" + x.value() + "_" + x.timestamp());
                });
//            consumer.commitSync();
                Set<TopicPartition> newPartitions = assignment.stream().filter(x -> !removePartitions.contains(x)).collect(Collectors.toSet());
                if (newPartitions.isEmpty()) {
                    System.out.println("finish receive msg");
                    return;
                } else {
                    consumer.assign(newPartitions);
                }
            }
        }finally {
            consumer.close();
        }
    }

    private Map<TopicPartition, Long> getGreaterOffset(KafkaConsumer<String, String> consumer, List<TopicPartition> topicPartitionList, ConsumerInfo consumerInfo, long time) {
        Map<TopicPartition, Long> endOffsetMap = consumer.endOffsets(topicPartitionList);
        Map<TopicPartition, Long> startMap = getPartitions(consumerInfo)
                .map(x -> new TopicPartition(consumerInfo.getTopicId(), x.partition()))
                .collect(Collectors.toMap(x -> x, x -> time));
        return consumer.offsetsForTimes(startMap)
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue() != null ? entry.getValue().offset() : endOffsetMap.get(entry.getKey())));
    }
}
