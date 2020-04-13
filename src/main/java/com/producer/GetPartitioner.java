package com.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class GetPartitioner implements Partitioner {

    //这里用于生产者指定分区发送使用
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //这里返回0号分区，是指只往0号分区发送数据
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
