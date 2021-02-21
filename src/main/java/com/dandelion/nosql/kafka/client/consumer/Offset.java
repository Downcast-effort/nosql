package com.dandelion.nosql.kafka.client.consumer;

/**
 * @author
 * @version 1.0
 * @date 2021/1/26 22:31
 */
public class Offset {

    private long begin;
    private long end;
    boolean isDeal;
    boolean isCommited;
    long runTime;

    public Offset(){
        this.isDeal =false;
        this.runTime = System.currentTimeMillis();
    }

    public Offset(long begin, long end, boolean isDeal, boolean isCommited, long runTime) {
        this.begin = begin;
        this.end = end;
        this.isDeal = false;
        this.runTime = runTime;
    }


    public long getBegin() {
        return begin;
    }

    public void setBegin(long begin) {
        this.begin = begin;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public boolean isDeal() {
        return isDeal;
    }

    public void setDeal(boolean deal) {
        isDeal = deal;
    }

    public boolean isCommited() {
        return isCommited;
    }

    public void setCommited(boolean commited) {
        isCommited = commited;
    }

    public long getRunTime() {
        return runTime;
    }

    public void setRunTime(long runTime) {
        this.runTime = runTime;
    }

    @Override
    public String toString() {
        return "Offset{" +
                "begin=" + begin +
                ", end=" + end +
                ", isDeal=" + isDeal +
                ", isCommited=" + isCommited +
                ", runTime=" + runTime +
                '}';
    }
}
