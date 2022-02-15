package com.hyy.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
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
//            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
            //自动提交offset的间隔时间，当ENABLE_AUTO_COMMIT_CONFIG=true时，才有意义
//            properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            //一次poll最大拉取消息的条数，可以根据消费速度的快慢来设置
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,500);
            //如果两次poll的时间如果超出了30s的时间间隔，kafka会认为其消费能力过弱，将其踢出消费组。将分区分配给其他消费者
            properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30*1000);

            //kafka如果超过10s没有收到消费者的心跳，则会把消费者踢出消费组，进行rebalance,将分区费赔给其他消费者
            properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10*1000);
            //consumer给broker发送心跳的间隔时间
            properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 30*1000);



            //创建一个消费者
            consumer = new KafkaConsumer<>(properties);
            //订阅主题列表
            consumer.subscribe(Arrays.asList(TOPIC));
            while(true){
                //长轮询poll消息
                //消费者一次poll500条消息，这里设置了长轮询的时间为1000ms
                //如果一次poll到了500条
                //如果一次没有poll到500条，且时间在1秒内，那么长轮询继续poll,要么poll到500条，要么到1s
                //如果多次poll都没到500条，且1秒时间到了， 那么直接执行for循环
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("收到消息：partition = %d, offset = %d, key = %s, value =%s%n",record.partition(),record.offset(),record.key(),record.value());
                }
                if(records.count()>0){
                    //手动同步提交offset,当前线程会阻塞知道offset提交成功
                    //一般使用同步提交，因为提交之后一般也没有什么逻辑代码了
//                    consumer.commitSync();

                    consumer.commitAsync(new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            if(e!=null){
                                System.err.println("Commit failed for " + map);
                                System.err.println("Commit failed exception " + Arrays.toString(e.getStackTrace()));
                            }else{
                                System.out.println("Commit offset success");
                            }
                        }
                    });

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
