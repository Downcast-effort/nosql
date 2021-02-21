package com.dandelion.nosql.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author
 * @version 1.0
 * @date 2021/1/26 21:50
 */
public class Result<K,V> {
    private boolean success;
    private Exception exception;
    private List<ConsumerRecord<K,V>> list;


    public Result(boolean success, Exception exception,List<ConsumerRecord<K,V>> recordList){
        this.success = success;
        this.exception = exception;
        this.list = recordList;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
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
