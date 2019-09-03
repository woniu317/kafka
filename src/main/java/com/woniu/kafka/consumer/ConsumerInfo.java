package com.woniu.kafka.consumer;

/**
 * @Author zhoushijie
 * @Date 09:43 2019-09-03
 * @Description
 */

@lombok.Data
public class ConsumerInfo {
    String topicId;
    String groupId;
    String brokers;
    Long startTime;
    Long endTime;

    public ConsumerInfo(String brokers, String topicId, String groupId ) {
        this.brokers = brokers;
        this.topicId = topicId;
        this.groupId = groupId;
    }

}
