package com.dandelion.nosql.kafka.client.consumer;

/**
 * @author
 * @version 1.0
 * @date 2021/1/31 19:28
 */
public abstract class KafkaAutoMsgListener<K,V> extends KafkaBatchMsgListener<K,V> {

    public KafkaAutoMsgListener(){

    }

    @Override
    public String getCommitType(){
        return "3";
    }
}


