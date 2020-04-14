package com.intercetor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 拦截器
 * 在消息进入队列之前，做一些拦截 或者 增加一些参数 例如时间戳之类的
 * <p>
 * 这个拦截器是个累加器
 */
public class CountIntercetor implements ProducerInterceptor<String, String> {

    private int successCount = 0;
    private int failCount = 0;

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //这里如果没有进行什么操作 一定记得返回原来的record回去
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            successCount++;
        } else {
            failCount++;
        }
    }

    public void close() {
        System.out.println("success条数:" + successCount);
        System.out.println("fail条数:" + failCount);
    }

    public void configure(Map<String, ?> configs) {

    }
}
