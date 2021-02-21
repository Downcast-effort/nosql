package com.dandelion.nosql.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;

/**
 * @author dandelion
 * @version 1.0
 * @date 2021/1/24 15:16
 */
public interface KafkaMsgListener<K,V> {

    String MANU_SYN_COMIT = "1";
    String MANU_CONCURRENT_COMIT = "2";
    String AUTO_COMIT = "3";
    String SAFE_AUTO_COMIT = "4";

    void onDoMessage(List<ConsumerRecord<K,V>> var1, KafkaConsumer var2);
    void onMessage(List<ConsumerRecord<K,V>> var1, KafkaConsumer var2);
    void onBeforeReBlance(KafkaConsumer var1, Collection<TopicPartition> var2);
    void onAfterReBlance(KafkaConsumer var1, Collection<TopicPartition> var2);
    void onNoData(KafkaConsumer var1);
    String getCommitType();

}
