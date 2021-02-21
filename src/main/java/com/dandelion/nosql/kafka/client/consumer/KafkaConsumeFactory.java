package com.dandelion.nosql.kafka.client.consumer;

import com.dandelion.nosql.kafka.util.Constant;
import com.dandelion.nosql.util.PropertyUtil;
import jdk.nashorn.internal.codegen.CompilerConstants;
import jdk.nashorn.internal.ir.LiteralNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author dandelion
 * @version 1.0
 * @date 2021/1/24 15:10
 */
public class KafkaConsumeFactory {

    private static final Logger log  = LoggerFactory.getLogger(KafkaConsumeFactory.class);
    private static final String DEFAULT_PULL_WAIT_TIME = "1000";
    private static final String KEY_PULL_WAIT_TIME = "dandelion.pull.wait.time";
    private static final String KEY_WORK_THREAD_NUM = "dandelion.work.thread.num";
    private static final String KEY_TOPICS = "dandelion.topics";
    private static final String KEY_TASK_MAX_TIME = "dandelion.task.max.time";
    private static final String KEY_ENABLE_AUTO_COMMIT = "enable.auto.commit";

    private static ConcurrentHashMap<String,String> containerMap = new ConcurrentHashMap<>();


    private static synchronized void create(Properties properties,KafkaMsgListener listener){
        String topics = properties.getProperty("dandelion.topics");
        if (StringUtils.isBlank(topics)){
            log.error("TOPIC不能为空");
            System.exit(0);
        }

        String isExists = containerMap.get("topics");
        if (isExists == null){
            if ("3".equals(listener.getCommitType())){
                properties.put("enable.auto.commit", "true");
            }else{
                properties.put("enable.auto.commit", "false");
            }
            int workThreadNum = NumberUtils.toInt(properties.getProperty("dandelion.work.thread.num", "1"));
            int pullWaitTime = NumberUtils.toInt(properties.getProperty("dandelion.pull.wait.time","1000"));
            int taskMaxTime = NumberUtils.toInt(properties.getProperty("dandelion.task.max.time","6000"));

            Constant.TASK_MAX_TIME = taskMaxTime;
            ThreadExecutorService.getInstance().init(workThreadNum);
            KafkaConsumeFactory.KafkaConsumerContainer kcr = new KafkaConsumeFactory.KafkaConsumerContainer(properties,listener,pullWaitTime,workThreadNum,taskMaxTime);
            new Thread(kcr).start();
            containerMap.put(topics,"1");
        }
    }

    public static synchronized void create(String propertiesPath,KafkaMsgListener listener){
        Properties properties = PropertyUtil.load(propertiesPath);
        create(properties,listener);

    }

    static class KafkaAutoConsumerTask<K,V> implements Runnable{

        private List<ConsumerRecord<K,V>> recordList;
        private KafkaMsgListener listener;
        private KafkaConsumer consumer;

        public KafkaAutoConsumerTask(List<ConsumerRecord<K,V>> recordList,KafkaMsgListener listener,KafkaConsumer consumer){
            this.recordList = recordList;
            this.listener = listener;
            this.consumer = consumer;
        }
        @Override
        public void run() {
            Thread.currentThread().setName("kafka-consume-" + Thread.currentThread().getId());
            try{
                this.listener.onMessage(this.recordList, this.consumer);
            }catch (Exception var2){
                KafkaConsumeFactory.log.error("KafkaAutoConsumeRunner-run-onMessage:",var2);
            }
        }
    }

    static class KafkaConcurrentManuConsumerTask<K,V> implements Callable<Result>{
        private List<ConsumerRecord<K,V>> recordList;
        private KafkaMsgListener listener;
        private KafkaConsumer consumer;

        public KafkaConcurrentManuConsumerTask(List<ConsumerRecord<K,V>> recordList,KafkaMsgListener listener,KafkaConsumer consumer){
            this.recordList = recordList;
            this.listener = listener;
            this.consumer = consumer;
        }

        @Override
        public Result call() throws  Exception{
            Thread.currentThread().setName("Kafka-concurr-" + Thread.currentThread().getId());
            try{
                this.listener.onDoMessage(this.recordList,this.consumer);
                Result result = new Result(true,null,null);
                return result;
            }catch (Exception var5){
                log.error("KafkaConcurrentManuConsumerTask-run-onMessages:",var5);
                Result result = new Result(false,var5,this.recordList);
                return result;
            }
        }
    }
    static class KafkaConcurrentAutoSafeConsumerTask<K,V> implements Runnable{

        private List<ConsumerRecord<K,V>> recordList;
        private KafkaConcurrencySafeAutoListener listener;
        private KafkaConsumer consumer;
        private Map<String,String> offsetMap;

        public KafkaConcurrentAutoSafeConsumerTask(List<ConsumerRecord<K,V>> recordList,Map<String,String> offsetMap,KafkaConcurrencySafeAutoListener listener,KafkaConsumer consumer){
            this.recordList = recordList;
            this.listener = listener;
            this.consumer  = consumer;
            this.offsetMap = offsetMap;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("kafka-concurrent-auto-" + Thread.currentThread().getId());
            try{
                this.listener.onDoMessage(this.recordList,this.offsetMap,this.consumer);
            }catch (Exception var2){
                log.error("KakfaConcurrentAutoSafeConsumerTask-run-onMessage:",var2);
            }
        }
    }

    static class KafkaConsumerContainer<K,V> implements Runnable{
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private KafkaConsumer consumer;
        private KafkaMsgListener listener;
        private int pullWaitTime;
        private int workThreadTime;
        private int taskMaxTime;

        public KafkaConsumerContainer(Properties properties,final KafkaMsgListener listener,int pullWaitTime,int workThreadTime,int taskMaxTime){
            this.listener = listener;
            this.pullWaitTime = pullWaitTime;
            this.consumer = this.consumer;
            this.workThreadTime = workThreadTime;
            this.taskMaxTime = taskMaxTime;
            String topics = properties.getProperty("dandelion.topics");
            this.consumer.subscribe(Arrays.asList(topics.split(",")), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    listener.onBeforeReBlance(KafkaConsumerContainer.this.consumer, partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    listener.onAfterReBlance(KafkaConsumerContainer.this.consumer,partitions);
                }
            });
        }

        @Override
        public void run() {
            Thread.currentThread().setName("kafka-consume-" + Thread.currentThread().getId());
            while(!this.closed.get()){
                try{
                    ConsumerRecords<K,V> records = this.consumer.poll(this.pullWaitTime);
                    if (records.isEmpty()){
                        this.listener.onNoData(this.consumer);
                        try{
                            Thread.sleep(1000L);
                        }catch (InterruptedException var3){
                            KafkaConsumeFactory.log.error("KafkaConsumeContainer-Thread.sleep:",var3);
                        }
                    }else if ("3".equals(this.listener.getCommitType())){
                        this.dealAuto(records);
                    }else if ("2".equals(this.listener.getCommitType())){
                        this.dealConcurrentManu(records);
                    }else if ("1".equals(this.listener.getCommitType())){
                        this.dealSyncronizeManu(records);
                    }else if ("4".equals(this.listener.getCommitType())){
                        this.dealConcurrencySafeAuto(records);
                    }

                }catch (Exception var4){
                    KafkaConsumeFactory.log.error("run:",var4);
                }
            }
            this.consumer.close();
        }


        public void dealAuto(ConsumerRecords records){
            int count = records.count();
            int between = count / this.workThreadTime;
            List<ConsumerRecord> recordList = new ArrayList<>();
            Iterator i$ = records.iterator();
            while(i$.hasNext()){
                ConsumerRecord<K,V> record = (ConsumerRecord) i$.next();
                recordList.add(record);
                if (recordList.size() == between){
                    ThreadExecutorService.executeCallable((Callable) new KafkaAutoConsumerTask(recordList,this.listener,this.consumer));
                    recordList = new ArrayList<>();
                }
            }
            if (recordList.size() > 0){
                ThreadExecutorService.executeCallable((Callable) new KafkaAutoConsumerTask(recordList,this.listener,this.consumer));
            }
        }

        public void dealSyncronizeManu(ConsumerRecords<K,V> records){
            try{
                List<ConsumerRecord> recordList = new ArrayList<>();
                Iterator i$ = records.iterator();
                while(i$.hasNext()){
                    ConsumerRecord  record = (ConsumerRecord) i$.next();
                    recordList.add(record);
                }
                this.listener.onDoMessage(recordList,this.consumer);
            }catch (Exception var5){
                log.error("dealSyncronizeManu-listener.onDoMessage:",var5);
            }
        }

        public void dealConcurrencySafeAuto(ConsumerRecords<K,V> records){
            KafkaConcurrencySafeAutoListener safeAutoListener = (KafkaConcurrencySafeAutoListener) this.listener;
            int count = records.count();
            double num = Double.valueOf(count) / Double.valueOf(this.workThreadTime);
            int between = count / this.workThreadTime;
            int aimNum = count % this.workThreadTime;
            List<ConsumerRecord<K,V>> recordList = new ArrayList<>();
            int indexNum = 0;
            Iterator i$ = records.iterator();
            while(i$.hasNext()){
                ConsumerRecord<K,V> record = (ConsumerRecord<K, V>) i$.next();
                recordList.add(record);
                int subSize = between;
                if (num < 1.0D){
                    subSize = 1;
                }else if (num > 1.0D && aimNum> 0){
                    if (indexNum <= aimNum){

                        subSize = between + 1;
                    }else{
                        subSize = between;
                    }
                }
                if (recordList.size() == subSize){
                    Map<String,String> offsetMap = safeAutoListener.beforeOnDoMessage(recordList,this.consumer);
                    ThreadExecutorService.executeRunnable(new KafkaConsumeFactory.KafkaConcurrentAutoSafeConsumerTask(recordList,offsetMap,safeAutoListener,this.consumer));
                    ++indexNum;
                    recordList = new ArrayList<>();
                }

            }

            if (recordList.size() > 0){
                Map<String,String> offsetMap = safeAutoListener.beforeOnDoMessage(recordList,this.consumer);
                ThreadExecutorService.executeRunnable(new KafkaConsumeFactory.KafkaConcurrentAutoSafeConsumerTask(recordList,offsetMap,safeAutoListener,this.consumer));
            }
            safeAutoListener.afterOnDoMessage(this.consumer);
        }

        public void dealConcurrentManu(ConsumerRecords<K,V> records){
            int count = records.count();
            double num = Double.valueOf((double) count / Double.valueOf((double) this.workThreadTime));
            int between = count / workThreadTime;
            int aimNum = count % this.workThreadTime;
            List<ConsumerRecord<K,V>> recordList = new ArrayList<>();
            List<List<ConsumerRecord<K,V>>> rangeList = new ArrayList<>();
            List<Future<Result>> futures = new ArrayList<>();

            int indexNum = 0;
            int sumNum = 0;
            Iterator i$ = records.iterator();
            int i;
            while(i$.hasNext()){
                ConsumerRecord<K,V> record = (ConsumerRecord<K, V>) i$.next();
                recordList.add(record);
                i = between;
                if (num < 1.0D){
                    i = 1;
                }else if (num > 1.0D && aimNum >0){
                    if (indexNum <= aimNum){
                        i = between + 1;
                    }else{
                        i = between;
                    }
                }
                if (recordList.size() == i){
                    sumNum += recordList.size();
                    futures.add(ThreadExecutorService.executeCallable(new KafkaConcurrentManuConsumerTask(recordList,this.listener,this.consumer)));
                    rangeList.add(recordList);
                    recordList = new ArrayList<>();
                    ++indexNum;
                }
            }
            if (recordList.size() > 0){
                int var10000 = sumNum + recordList.size();
                futures.add(ThreadExecutorService.executeCallable(new KafkaConcurrentManuConsumerTask(recordList,this.listener,this.consumer)));
                rangeList.add(recordList);
            }


            List<ConsumerRecord<K,V>> succRecordList = new ArrayList<>();
            List<ErrorInfo> errorResults = new ArrayList<>();

            i = 0;
            long startTime = System.currentTimeMillis();
            for ( i$ = futures.iterator();i$.hasNext();++i){
                Future future = (Future) i$.next();
                try{
                    Result result = (Result) future.get((long)this.taskMaxTime, TimeUnit.SECONDS);
                    if (!result.isSuccess()){
                        errorResults.add(new ErrorInfo(result.getException(),rangeList.get(i)));
                    }else {
                        succRecordList.addAll((Collection)rangeList.get(i));
                    }
                }catch (Exception var23){
                    ErrorInfo errorInfo = new ErrorInfo(var23,rangeList.get(i));
                    errorResults.add(errorInfo);
                    log.error("KafkaMsgListenter-Future-get:",var23);

                    try{
                        future.cancel(true);
                    }catch (Exception var22){
                        log.error("KafkaMsgListener-Future.cancel:",var22);
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            if (endTime -startTime >= (long)this.taskMaxTime * 1000){
                log.error("任务时间过长：" + (endTime - startTime) + "毫秒");
            }
            KafkaConcurrentManuMsgListener kafkaConcurrentManuMsgListener = (KafkaConcurrentManuMsgListener) this.listener;
            KafkaConcurrentManuMsgListener.onEnd(errorResults,this.consumer);
        }
    }


}
