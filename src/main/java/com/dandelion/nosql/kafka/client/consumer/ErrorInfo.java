package com.dandelion.nosql.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * @author
 * @version 1.0
 * @date 2021/1/26 22:01
 */
public class ErrorInfo<K,V> {

    private Exception exception;
    private List<ConsumerRecord<K,V>> list;

    public ErrorInfo(Exception exception, List<ConsumerRecord<K, V>> list) {
        this.exception = exception;
        this.list = list;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public List<ConsumerRecord<K, V>> getList() {
        return list;
    }

    public void setList(List<ConsumerRecord<K, V>> list) {
        this.list = list;
    }
}
