package com.woniu.kafka.consumer.rebalance;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * @Author zhoushijie
 * @Date 10:25 2019-09-03
 * @Description 消费重新平衡
 */
public class Rebalance implements ConsumerRebalanceListener {
    private Consumer<?, ?> consumer;

    public Rebalance(Consumer<?, ?> consumer) {
        this.consumer = consumer;
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("onPartitionRevoked...");
        partitions.forEach(x -> System.out.println("partition:" + x.partition() + ", offset:" + consumer.position(x)));
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("onPartitionsAssigned...");
        partitions.forEach(x -> System.out.println("partition:" + x.partition() + ", offset:" + consumer.position(x)));
    }
}
