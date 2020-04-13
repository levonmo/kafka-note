package com.customer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 低级api
 * <p>
 * 根据指定的topic，partition，offset来获取数据
 */

public class LowerConsumer {

    public static void main(String[] args) {
        //定义相关参数
        ArrayList<String> brokers = new ArrayList();
        brokers.add("127.0.0.1:9092");
        brokers.add("127.0.0.1:9093");
        brokers.add("127.0.0.1:9094");
        //主题
        String topic = "test2";
        //分区
        int partition = 0;
        //offset
        long offset = 2;

        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(brokers, topic, partition, offset);
    }

    //找分区leader,找那个topic以及几号分区的领导
    private BrokerEndPoint findLeader(List<String> brokers, String topic, int partition) {
        for (String broker : brokers) {
            String[] hostAndPort = broker.split(":");
            SimpleConsumer getLeder = new SimpleConsumer(hostAndPort[0], Integer.valueOf(hostAndPort[1]), 1000, 1024 * 4, "getLeder");
            //这里的topic是可以传递多个的
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            TopicMetadataResponse metadataResponse = getLeder.send(topicMetadataRequest);
            List<TopicMetadata> topicMetadata = metadataResponse.topicsMetadata();
            //所以这里拿到的是一个topic list
            for (TopicMetadata topicMetadatum : topicMetadata) {
                //因为每个topic可以存在多个分区，所以返回的是 partition list
                List<PartitionMetadata> partitionMetadata = topicMetadatum.partitionsMetadata();
                for (PartitionMetadata partitionMetadatum : partitionMetadata) {
                    if (partition == partitionMetadatum.partitionId()) {
                        return partitionMetadatum.leader();
                    }
                }
            }
        }
        return null;
    }

    //获取数据
    private void getData(List<String> brokers, String topic, int partition, long offset) {
        BrokerEndPoint leader = findLeader(brokers, topic, partition);
        if (leader == null) {
            return;
        }
        String host = leader.host();
        int port = leader.port();
        SimpleConsumer getData = new SimpleConsumer(host, port, 1000, 1024 * 4, "getData");
        //fetchSize：100 表示的是一个字节数
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 100).build();
        FetchResponse fetchResponse = getData.fetch(fetchRequest);
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            //这里获取的offset可以保存在本地mysql获取zookeeper任何可以存放数据的地方
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1 + "--" + new String(bytes));
        }
    }

}
