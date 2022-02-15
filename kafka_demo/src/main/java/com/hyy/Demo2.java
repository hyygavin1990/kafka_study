package com.hyy;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 同步发送消息
 */
public class Demo2 {
    private static final String TOPIC="education-info";
    private static final String BROKER_LIST="localhost:9092";

    public static void main(String[] args){
        KafkaProducer<String,String> producer = null;
        try{
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
            properties.put(ProducerConfig.ACKS_CONFIG,"all");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
            producer = new KafkaProducer<String, String>(properties);

            String message = "hello world";
            ProducerRecord<String,String> record = new ProducerRecord<String,String>(TOPIC,message);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null==exception){
                        System.out.println("perfect!");
                    }
                    if(null!=metadata){
                        System.out.print("offset:"+metadata.offset()+";partition:"+metadata.partition());
                    }
                }
            }).get();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (producer != null) {
                producer.close();
            }
        }
    }
}
