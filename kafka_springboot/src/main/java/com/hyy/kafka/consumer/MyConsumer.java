package com.hyy.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MyConsumer {

    //针对每一条记录
    @KafkaListener(topics = "my-replicated-topic",groupId = "MyGroup1")
    public void listenGroup(ConsumerRecord<String,String> record, Acknowledgment ack){
        String value = record.value();
        System.out.println("MyGroup1----"+value);
        System.out.println(record);
        //手动提交offset
        ack.acknowledge();
    }

    //针对一批数据
    @KafkaListener(topics = "my-replicated-topic",groupId = "MyGroup20")
    public void listensGroup(List<String> records, Acknowledgment ack){
        for (String record : records) {
            System.out.println("MyGroup20----"+record);
        }
        //手动提交offset
        ack.acknowledge();
    }

    //针对一批数据
    @KafkaListener(topics = {"my-replicated-topic"},groupId = "MyGroup22222", containerFactory = "batchFactory")
    public void listen(ConsumerRecords<String, String> records,Acknowledgment ack){
        for (ConsumerRecord<?,?> record : records) {
            System.out.println("MyGroup22222----"+record.value());
            System.out.printf("收到消息：partition = %d, offset = %d, key = %s, value =%s%n",record.partition(),record.offset(),record.key(),record.value());
        }
        //手动提交offset
        ack.acknowledge();
    }


}
