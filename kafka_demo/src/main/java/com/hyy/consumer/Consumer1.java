package com.hyy.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 消费者示例
 */
public class Consumer1 {

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
            //properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
            //自动提交offset的间隔时间，当ENABLE_AUTO_COMMIT_CONFIG=true时，才有意义
            //properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            //创建一个消费者
            consumer = new KafkaConsumer<>(properties);
            //订阅主题列表
            consumer.subscribe(Arrays.asList(TOPIC));
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("收到消息：partition = %d, offset = %d, key = %s, value =%s%n",record.partition(),record.offset(),record.key(),record.value());
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
