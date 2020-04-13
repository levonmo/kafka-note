package com.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 有回调行函数
 */
public class SimpleProducerWithCallback {

    public static void main(String[] args) {
        Properties props = new Properties();
        //broker地址
        props.put("bootstrap.servers", "127.0.0.1:9092");
        //acks应答机制，acks=0/1/-1
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("test2", Integer.toString(i)), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        //分区+偏移量
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
