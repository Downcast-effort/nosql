package com.dandelion.nosql.kafka.client.consumer;

import com.dandelion.nosql.kafka.util.Constant;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author
 * @version 1.0
 * @date 2021/1/26 22:33
 */
public class PartitionOffset {

    private  static final Logger log = LoggerFactory.getLogger(PartitionOffset.class);
    private static final int MAX_OFFSET_SIZE = 500;
    private static final int MAX_COMMIT_INTEVAL = 5000;
    private List<Offset> offsetList;
    private ReentrantReadWriteLock lock = null;
    private long lastCommitTime;
    private TopicPartition topicPartition;

    public PartitionOffset(String topic,int partition){
        this.lock = new ReentrantReadWriteLock();
        this.offsetList = new ArrayList<>();
        this.lastCommitTime = System.currentTimeMillis();
        this.topicPartition = new TopicPartition(topic,partition);
    }

    public List<Offset> getOffsetList(){
        return this.offsetList;
    }

    public long getLastCommitTime(){
        return this.lastCommitTime;
    }

    public int size (){
        return this.offsetList.size();
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public void setTopicPartition(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    @Override
    public String toString() {
        return "PartitionOffset{" +
                "offsetList=" + offsetList +
                ", lock=" + lock +
                ", lastCommitTime=" + lastCommitTime +
                ", topicPartition=" + topicPartition +
                '}';
    }

    public void addOffset(Offset offset){
        this.lock.readLock().lock();
        try{
            this.offsetList.add(offset);
        }finally {
            this.lock.readLock().unlock();
        }
    }

    public Offset getCommitOffset(){
        if(this.offsetList == null || this.offsetList.size() == 0 || this.offsetList.size() < 500 && System.currentTimeMillis() - this.lastCommitTime < 5000L){
            return  null;
        }else{
            this.lock.writeLock().lock();

            try{
                boolean lastIsDeal = false;
                boolean isFirstIsDeal = true;
                Offset lastDealOffset = null;
                Offset commitOffset = null;
                int i;
                Offset offset;
                for(i = 0;i<this.offsetList.size();++i){
                    offset = this.offsetList.get(i);
                    if (!offset.isDeal && System.currentTimeMillis() - offset.getRunTime() >= Constant.TASK_MAX_TIME * 1000){
                        offset.setDeal(true);
                    }
                }
                for(i = 0;i<this.offsetList.size();++i){
                    offset = this.offsetList.get(i);
                    if(offset.isDeal()){
                        if (isFirstIsDeal){
                            commitOffset = offset;
                            offset.setCommited(true);
                        }

                        if (lastIsDeal && lastDealOffset != null){
                            if (offset.getBegin() - lastDealOffset.getEnd() == 1L){
                                offset.setBegin(lastDealOffset.getBegin());
                                this.offsetList.remove(i-1);
                                --i;
                            }else{
                                log.error("Kafka组件遇到不连续的offset");
                            }
                        }
                        lastDealOffset = offset;
                        lastIsDeal = true;
                    }else{
                        isFirstIsDeal = false;
                        lastIsDeal = false;
                        lastDealOffset = null;
                    }
                }
                Offset var10 = commitOffset;
                return var10;
            }finally {
                this.lock.writeLock().unlock();
            }
        }
    }

    public long cleanCommitOffset(){
        this.lock.writeLock().lock();
        long commitRange = 0L;
        try{
            int i = 0;
            while(true){
                if (i< this.offsetList.size()){
                    Offset offset = this.offsetList.get(i);
                    if (offset.isCommited()){
                        commitRange += offset.getEnd() - offset.getBegin() + 1L;
                        this.offsetList.remove(i);
                        --i;
                        ++i;continue;
                    }
                }
                this.lastCommitTime = System.currentTimeMillis();
                return commitRange;
            }
        }finally {
            this.lock.writeLock().unlock();
        }
    }

}
