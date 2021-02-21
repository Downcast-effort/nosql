package com.dandelion.nosql.kafka.client.consumer;

import javafx.beans.binding.StringBinding;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author
 * @version 1.0
 * @date 2021/1/26 22:28
 */
public abstract class KafkaConcurrencySafeAutoListener<K,V> extends KafkaBatchMsgListener<K,V> {
    private static final Logger log = LoggerFactory.getLogger(KafkaConcurrencySafeAutoListener.class);
    private ConcurrentHashMap<String,PartitionOffset> offsetMap = new ConcurrentHashMap<>();
    private static final String SPLIT_CHAR = "$";
    private boolean hasNotInit = true;
    private Thread monitorThread = null;
    private AtomicLong recNum = new AtomicLong();
    private AtomicLong commitNum = new AtomicLong();

    public KafkaConcurrencySafeAutoListener(){

    }

    public void init(KafkaConsumer consumer){
        if (this.monitorThread == null){
            this.monitorThread = new Thread(new Runnable(){
                @Override
                public void run(){
                    while(true){
                        try{
                            Thread.sleep(60000L);
                        }catch (InterruptedException var2){
                            log.error("",var2);
                        }
                        log.info("Kafka组件已接受数量：" + KafkaConcurrencySafeAutoListener.this.recNum.get() + "|已提交的数量：" +
                                KafkaConcurrencySafeAutoListener.this.commitNum.get());
                    }
                }
            });

            this.monitorThread.setName("Kafka_moniotor_" + this.monitorThread.getId());
            this.monitorThread.start();
        }

        this.recNum.set(0L);
        this.commitNum.set(0L);
        this.offsetMap = new ConcurrentHashMap<>();
        Set<TopicPartition> assignPartition = consumer.assignment();
        Iterator i$ = assignPartition.iterator();
        while(i$.hasNext()){
            TopicPartition topicPartition = (TopicPartition) i$.next();
            String key = topicPartition.topic() + "$" + topicPartition.partition();
            PartitionOffset partitionOffset = this.offsetMap.get(key);
            if (partitionOffset == null){
                partitionOffset = new PartitionOffset(topicPartition.topic(),topicPartition.partition());
                this.offsetMap.put(key,partitionOffset);
            }
        }
        log.info("初始化完毕：" + this.offsetMap);
    }

    public Map<String, Offset> beforeOnDoMessage(List<ConsumerRecord> recordList, KafkaConsumer consumer){
        if (this.hasNotInit){
            this.hasNotInit = false;
            this.init(consumer);
        }
        this.recNum.addAndGet(recordList.size());
        Map<String,Offset> offsetMapTemp = new HashMap<>();
        Iterator i$ = recordList.iterator();
        String key;
        Offset offset = null;
        while(i$.hasNext()){
            ConsumerRecord record = (ConsumerRecord) i$.next();
            key = record.topic() + "$" + record.partition();
            offset = offsetMapTemp.get(key);
            if (offset == null){
                offset = new Offset();
                offsetMapTemp.put(key,offset);
                offset.setBegin(record.offset());
                offset.setEnd(record.offset());
            }else if (record.offset() > offset.getEnd()){
                offset.setEnd(record.offset());
            }
        }

        PartitionOffset partitionOffset = null;
        for(i$ = offsetMapTemp.entrySet().iterator();i$.hasNext();partitionOffset.addOffset(offset)){
            Map.Entry<String,Offset> entry = (Map.Entry<String, Offset>) i$.next();
            key = entry.getKey();
            offset = entry.getValue();
            String [] arr = key.split("[$]");
            partitionOffset = this.offsetMap.get(key);
            if (partitionOffset == null){
                partitionOffset = new PartitionOffset(arr[0],Integer.parseInt(arr[1]));
                this.offsetMap.put(key,partitionOffset);
            }
        }
        this.commitOffset(consumer);
        return offsetMapTemp;
    }

    private void commitOffset(KafkaConsumer consumer){
        if (this.offsetMap!= null && this.offsetMap.size() != 0){
            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
            Iterator i$ = this.offsetMap.entrySet().iterator();
            while(i$.hasNext()){
                Map.Entry<String,PartitionOffset> entry = (Map.Entry<String, PartitionOffset>) i$.next();
                PartitionOffset partitionOffset = entry.getValue();
                Offset commitOffset = partitionOffset.getCommitOffset();
                offsetAndMetadataMap.put(partitionOffset.getTopicPartition(),new OffsetAndMetadata(commitOffset.getEnd() + 1L));
            }
            if (offsetAndMetadataMap.size() > 0){
                try{
                    consumer.commitAsync((OffsetCommitCallback) offsetAndMetadataMap);
                    StringBuilder sb = new StringBuilder();
                    i$ = offsetAndMetadataMap.entrySet().iterator();
                    Map.Entry entry;
                    while(i$.hasNext()){
                        entry = (Map.Entry) i$.next();
                        sb.append(((TopicPartition)entry.getValue()).topic() + "-" + ((TopicPartition)entry.getValue()).partition() +
                                ((OffsetAndMetadata)entry.getValue()).offset() + ",");
                    }
                    log.info("已经提交的偏移量：",sb);
                    i$ = this.offsetMap.entrySet().iterator();
                    while(i$.hasNext()){
                        entry = (Map.Entry) i$.next();
                        PartitionOffset partitionOffset = (PartitionOffset) entry.getValue();
                        this.commitNum.addAndGet(partitionOffset.cleanCommitOffset());
                    }
                }catch (Exception var7){
                    log.error("提交偏移量异常：" + offsetAndMetadataMap,var7);
                }
            }
        }
    }

    public final void onBeforeReblance(KafkaConsumer consumer,Collection<TopicPartition> partitions){
        log.info("onBeforeReblance 线程名称：" + Thread.currentThread().getName());
    }

    public final void onAfterReblance(KafkaConsumer consumer, Collection<TopicPartition> partitions){
        log.info("onAfterReblance 线程名称：" + Thread.currentThread().getName());
        this.hasNotInit = true;
    }


    public final void onDoMessage(List<ConsumerRecord<K,V>> recordList,Map<String,Offset> offsetMap,KafkaConsumer consumer){
        Iterator i$ = offsetMap.entrySet().iterator();
        while (i$.hasNext()){
            Map.Entry<String,Offset> entry = (Map.Entry<String, Offset>) i$.next();
            entry.getValue().setRunTime(System.currentTimeMillis());
        }

        super.onDoMessage(recordList,consumer);
        this.addDealEndOffset(offsetMap);
    }

    public void afterOnDoMessage(KafkaConsumer consumer){
        this.commitOffset(consumer);
    }

    public boolean addDealEndOffset(Map<String,Offset> offsetMap){
        Iterator i$ = offsetMap.entrySet().iterator();
        while(i$.hasNext()){
            Map.Entry<String,Offset> entry = (Map.Entry<String, Offset>) i$.next();
            entry.getValue().setDeal(true);
        }

        return true;
    }

    @Override
    public void onNoData(KafkaConsumer consumer){
        this.commitOffset(consumer);
    }


    @Override
    public String getCommitType(){
        return "4";
    }
}
