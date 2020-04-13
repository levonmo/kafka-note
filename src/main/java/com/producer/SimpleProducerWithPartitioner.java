package com.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 可以指定partitioner发送数据
 */
public class SimpleProducerWithPartitioner {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //指定分区,通过指定类的partition方法可以获取发送的分区号
        props.put("partitioner.class", "com.producer.GetPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("test2", Integer.toString(i)), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        //分区+偏移量
                        //看到结果只是往0号分区发送数据了
                        System.out.println(metadata.partition() + "--" + metadata.offset());
                    } else {
                        System.out.println("fail");
                    }
                }
            });
        }
        producer.close();
    }
}
