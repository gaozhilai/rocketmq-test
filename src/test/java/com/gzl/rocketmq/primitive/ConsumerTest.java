package com.gzl.rocketmq.primitive;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import com.gzl.rocketmq.base.ConfigConst;

import lombok.SneakyThrows;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/7/31 10:46
 */
public class ConsumerTest {

    /**
     * 常规的普通消费者
     */
    @Test
    @SneakyThrows
    public void testConsumer() {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(ConfigConst.CONCUMER_GROUP);

        // 设置NameServer的地址
        consumer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe(ConfigConst.TOPIC_NAME, "*");
        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody(), "utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
        new CountDownLatch(1).await();
    }
}
