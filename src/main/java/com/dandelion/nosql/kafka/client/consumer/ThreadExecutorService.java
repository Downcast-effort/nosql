package com.dandelion.nosql.kafka.client.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @author dandelion
 * @version 1.0
 * @date 2021/1/25 21:29
 */
public class ThreadExecutorService {
    private static final Logger log = LoggerFactory.getLogger(ThreadExecutorService.class);

    private static volatile ThreadExecutorService instance = null;
    private static final int   QUEUESIZE = 150;
    private static final long KEEPLIVETIME = 5L;
    private static final BlockingDeque<Runnable> QUEUE = (BlockingDeque<Runnable>) new ArrayBlockingQueue(150);
    private static final RejectedExecutionHandler CALLERRUNSHANDLER = new ThreadPoolExecutor.AbortPolicy();

    private ExecutorService boundedThreadPool;


    private ThreadExecutorService(){

    }
    public void init(int maxPoolSize){
        this.boundedThreadPool = new ThreadPoolExecutor(maxPoolSize, maxPoolSize, 5L,TimeUnit.SECONDS, QUEUE,CALLERRUNSHANDLER);
    }

    public static ThreadExecutorService getInstance() {
        if (null == instance) {
            Class var0 = ThreadExecutorService.class;
            synchronized (ThreadExecutorService.class) {
                if (null == instance) {
                    instance = new ThreadExecutorService();
                }
            }
        }
        return instance;
    }

    public static ExecutorService getThreadExecutor(){
        return getInstance().boundedThreadPool;
    }

    public static void executeRunnable(Runnable runnable){
        int i = 0;
        while(true){
            try{
                getInstance().boundedThreadPool.execute(runnable);
                return;
            }catch (RejectedExecutionException var5){
                if (i == 3){
                    try{
                        Thread.sleep(200L);
                    }catch (InterruptedException var4){
                        log.error("",var4);
                    }
                    i = 0;
                }
                ++i;
            }
        }
    }

    public static Future executeCallable(Callable callable){
        int i = 0;
        while(true){
            try{
                return getInstance().boundedThreadPool.submit(callable);
            }catch (RejectedExecutionException var5){
                if (i == 3){
                    try{
                        Thread.sleep(200L);
                    }catch (InterruptedException var4){
                        log.error("",var4);
                    }
                    i = 0;
                }
                ++i;
            }
        }
    }
}
