package com.woniu.kafka.producer;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("mypartition>>>");
        if (key == null)
            return 0;

        List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
        if (availablePartitions == null || availablePartitions.size() <= 0)
            return 0;

        int partitionKey = Integer.parseInt((String) key);

        int partitionSize = availablePartitions.size();

        return availablePartitions.get(partitionKey % partitionSize).partition();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("hell configure");
    }
}
