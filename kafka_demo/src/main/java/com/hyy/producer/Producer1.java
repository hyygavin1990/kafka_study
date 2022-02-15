package com.hyy.producer;

import com.alibaba.fastjson.JSON;
import com.hyy.entity.Order;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 同步发送消息
 */
public class Producer1 {

    //主题
    private static final String TOPIC="my-replicated-topic";

    //集群节点
    private static final String BROKER_LIST="192.168.3.200:9092,192.168.3.200:9093,192.168.3.200:9094";

    public static void main(String[] args){
        KafkaProducer<String,String> producer = null;
        try{
            Properties properties = new Properties();
            //集群节点
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
            //设置发送成功确认方式（ack）
            /*
              ack等于0：意味着producer不等待broker同步完成的确认，继续发送下一条(批)信息
              提供了最低的延迟。但是最弱的持久性，当服务器发生故障时，就很可能发生数据丢失。例如leader已经死亡，producer不知情，还会继续发送消息broker接收不到数据就会数据丢失。

              ack等于1（默认）：意味着producer要等待leader成功收到数据并得到确认，才发送下一条message。此选项提供了较好的持久性较低的延迟性。
              Partition的Leader死亡，follower尚未复制，数据就会丢失。

              ack等于-1 或者 all ：意味着producer得到follower确认，才发送下一条数据
             */
            properties.put(ProducerConfig.ACKS_CONFIG,"all");
            //把发送的key序列化成字节数组
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            //把发送的value序列化成字节数组
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
            producer = new KafkaProducer<String, String>(properties);
            Order order = new Order(1L,1L);
            String message = JSON.toJSONString(order);
//            ProducerRecord<String,String> record = new ProducerRecord<String,String>(TOPIC,message);
            //未指明分区的情况，根据发送的key来确定分区，具体公式为 hash(key)%partitionNum
            ProducerRecord<String,String> record = new ProducerRecord<String,String>(TOPIC,"mykey",message);
            RecordMetadata metaData = producer.send(record).get();
            System.out.println("同步方式发送消息：" + "topic-"+metaData.topic() + "|partition-" + metaData.partition() + "|offset-" + metaData.offset());

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (producer != null) {
                producer.close();
            }
        }
    }
}
