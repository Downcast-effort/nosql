package com.dandelion.nosql.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;

/**
 * @author
 * @version 1.0
 * @date 2021/1/26 22:19
 */
public  abstract class KafkaConcurrentManuMsgListener<K,V> extends KafkaBatchMsgListener<K,V> {
    public KafkaConcurrentManuMsgListener(){

    }

    @Override
    public String getCommitType(){
        return "2";
    }

    @Override
    public void onDoMessage(List<ConsumerRecord<K,V>> recordList, KafkaConsumer consumer){
        this.onMessage(recordList,consumer);
    }

    public boolean isStop(){
        return Thread.currentThread().isInterrupted();
    }

    public static void onEnd(List<ErrorInfo> var1, KafkaConsumer var2) {
    }

}
