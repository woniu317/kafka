package com.woniu.kafka.consumer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author zhoushijie
 * @Date 17:39 2019-09-03
 * @Description
 */
public interface StopConsumer {
    AtomicBoolean stopConsumer = new AtomicBoolean(false);
}
