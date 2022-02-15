package com.hyy.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 消费者示例
 */
public class Consumer2 {

    //
    private static final String TOPIC="my-replicated-topic";
    //集群节点
    private static final String BROKER_LIST="192.168.3.200:9092,192.168.3.200:9093,192.168.3.200:9094";
    private static final String CONSUMER_GROUP_NAME="testGroup";

    public static void main(String[] args){
        KafkaConsumer<String,String> consumer = null;
        try{
            Properties properties = new Properties();
            //集群节点
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
            //设置消费组
            properties.put(ConsumerConfig.GROUP_ID_CONFIG,CONSUMER_GROUP_NAME);
            //设置是否自动提交offset,默认是true
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
//            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
            //自动提交offset的间隔时间，当ENABLE_AUTO_COMMIT_CONFIG=true时，才有意义
//            properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            //一次poll最大拉取消息的条数，可以根据消费速度的快慢来设置
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,500);
            //如果两次poll的时间如果超出了30s的时间间隔，kafka会认为其消费能力过弱，将其踢出消费组。将分区分配给其他消费者
            properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30*1000);

            //kafka如果超过30s没有收到消费者的心跳，则会把消费者踢出消费组，进行rebalance,将分区费赔给其他消费者
            properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30*1000);
            //consumer给broker发送心跳的间隔时间
            properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10*1000);

            //创建一个消费者
            consumer = new KafkaConsumer<>(properties);
            //回溯到指定时间点
            long fetchDataTime = new Date().getTime()-1000*60*60;
            List<PartitionInfo> topPartitions = consumer.partitionsFor(TOPIC);
            Map<TopicPartition,Long> map = new HashMap<>();
            for (PartitionInfo par : topPartitions) {
                map.put(new TopicPartition(TOPIC,par.partition()),fetchDataTime);
            }
            Map<TopicPartition, OffsetAndTimestamp> parMap = consumer.offsetsForTimes(map);
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : parMap.entrySet()) {
                TopicPartition key = entry.getKey();
                OffsetAndTimestamp value = entry.getValue();
                if( key == null || value == null) continue;
                Long offset = value.offset();
                System.out.println("partition-" + key.partition() + "|offset-" + offset);
                System.out.println();
                if(value!=null){//根据消费力的timestamp确定offset
                    consumer.assign(Arrays.asList(key));
                    consumer.seek(key,offset);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("收到消息：partition = %d, offset = %d, key = %s, value =%s%n",record.partition(),record.offset(),record.key(),record.value());
                    }
                    if(records.count()>0){
                        //手动同步提交offset,当前线程会阻塞知道offset提交成功
                        //一般使用同步提交，因为提交之后一般也没有什么逻辑代码了
                        consumer.commitSync();
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (consumer != null) {
                consumer.close();
            }
        }

    }
}
