package com.dandelion.nosql.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;

/**
 * @author
 * @version 1.0
 * @date 2021/1/26 22:15
 */
public abstract class KafkaBatchMsgListener<K,V> implements KafkaMsgListener<K,V> {
    public KafkaBatchMsgListener(){

    }

    @Override
    public void onDoMessage(List<ConsumerRecord<K,V>> recordList, KafkaConsumer consumer){
        this.onMessage(recordList,consumer);
    }

    @Override
    public void onBeforeReBlance(KafkaConsumer consumer, Collection<TopicPartition> partitions){

    }

    @Override
    public void  onAfterReBlance(KafkaConsumer consumer, Collection<TopicPartition> partitions){

    }

    @Override
    public void onNoData(KafkaConsumer var1) {

    }
}
