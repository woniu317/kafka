# kafka
## 发送kafka消息:
- 实现类: com.woniu.kafka.producer.SendProducer

## 自定义分区规则 
- 实现类: com.woniu.kafka.producer.MyPartitioner

## 两种订阅模式:
- 实现类: com.woniu.kafka.consumer.Consumer
- 订阅模式一: 订阅topic，程序根据topic自动获取其所有的partitions
- 订阅模式二: 订阅partition，该方式下除指定topic外还要指定partitions

## 订阅组任务重新分配

 实现类:com.woniu.kafka.consumer.rebalance.Rebalance. 有新的机器加入或者停止消费时, 消费组在调用pool时先调用onPartitionsRevoked
 和onPartitionsAssigned, 然后再拉去数据, 解决了消息重复分发以及丢失的问题.

## 按照时间消费

实现类: com.woniu.kafka.consumer.backup.BackupForTimestamp, 可以指定消费开始时间亦或者消费区间对数据进行消费，
主要解决异常数据恢复的问题.

