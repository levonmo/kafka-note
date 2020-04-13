package com.customer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SimpeConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "localhost:9092");
        //消费者组id
        props.put("group.id", "test");
        //设置自动提交
        props.put("enable.auto.commit", "true");
        //提交延时
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        //指定topic,test1,test2
        consumer.subscribe(Arrays.asList("test1", "test2"));
        while (true) {
            //消费者拉取数据，每隔100ms拉取一次数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic() + "--" + record.partition() + "--" + record.value());
            }
        }
    }

}
