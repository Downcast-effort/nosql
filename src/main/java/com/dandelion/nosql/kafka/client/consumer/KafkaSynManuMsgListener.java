package com.dandelion.nosql.kafka.client.consumer;

/**
 * @author
 * @version 1.0
 * @date 2021/1/31 19:26
 */
public abstract class KafkaSynManuMsgListener<K,V> extends KafkaBatchMsgListener<K,V> {

    public KafkaSynManuMsgListener(){

    }

    @Override
    public String getCommitType(){
        return "1";
    }
}
