package com.intercetor;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 拦截器
 * 在消息进入队列之前，做一些拦截 或者 增加一些参数 例如时间戳之类的
 *
 * 这个拦截器是对消息增加了时间戳
 */
public class TimeIntercetor implements ProducerInterceptor<String, String> {

    //在数据进入kafka之前可以在这里对数据进行处理
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<String, String>(record.topic(), record.key(), System.currentTimeMillis() + "," + record.value());
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
