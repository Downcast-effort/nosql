package com.dandelion.nosql.kafka.client.producer;

import com.dandelion.nosql.util.PropertyUtil;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dandelion
 * @Description Kafka生产者
 * @version 1.0
 * @date 2021/1/23 15:31
 */

@ConditionalOnProperty(prefix = "kafka-producer")
public class ProducerMsgDomo {
    private static Logger log = LoggerFactory.getLogger(ProducerMsgDomo.class);

    private static Producer<String,String> producer;
    private static AtomicInteger sendInteger = new AtomicInteger(0);
    private static AtomicInteger callbackInteger = new AtomicInteger(0);

    /**
     *初始化生产者对象
     */
    public static void Init(){
        if (producer == null){
            producer = new KafkaProducer<String, String>(PropertyUtil.load("kafka-producer.properties"),
                    new StringSerializer(),new StringSerializer());
        }
    }


    /**
     * 发送消息
     * @param topic
     * @param key
     * @param value
     */
    public static void send(boolean flag, String topic,String key,String value){
        //同步发送，必须等到服务端响应后才能继续发送下一条消息
        long now = System.currentTimeMillis();
        if (flag){
            producer.send(new ProducerRecord<>(topic,key,value));
        }else{
            //异步的发送一条消息
            producer.send(new ProducerRecord<>(topic,key,value),
                    new Callback(){
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e){
                            if (null != e){
                                e.printStackTrace();
                            }else{
                                callbackInteger.incrementAndGet();
                                log.info(metadata.partition() + " " + metadata.offset());
                            }
                        }
                    });
        }
        log.info(String.format("异步生产第【%d】条长度为【%d】的消息耗时【%d】ms", sendInteger.incrementAndGet(),
                value.length(),System.currentTimeMillis() - now));

    }

}
