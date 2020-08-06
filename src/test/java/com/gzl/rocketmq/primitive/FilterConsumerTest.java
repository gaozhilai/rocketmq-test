package com.gzl.rocketmq.primitive;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
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
 * @date 2020/8/5 15:39
 */
public class FilterConsumerTest {

    /**
     * tag方式过滤通过服务器端与消费端配合实现
     * tag方式优点是简单简洁.
     * 但是每条消息标签只能有一个, 限制了过滤逻辑的复杂度. 同时可能会有非目标消息发送到消费端造成带宽浪费
     *
     * 原理: broker端会从consume queue中找到与消费端订阅tag的hashcode一致的消息发送到消费端
     * 消费端在消费时才真正比较tag内容, 这样可能存在hashcode碰撞而非目标消息被发送到消费端, 然后消费端再进行最终过滤确保准确性
     */
    @Test
    @SneakyThrows
    public void testTagFilterConsumer() {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(ConfigConst.CONCUMER_GROUP);

        // 设置NameServer的地址
        consumer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe(ConfigConst.TOPIC_NAME, "TagA || TagC || TagD");
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

    /**
     * 消息支持在消费者端设置sql92进行消息过滤, 过滤过程由broker端完成
     * broker需要添加如下配置允许sql过滤
     * enablePropertyFilter=true
     */
    @Test
    @SneakyThrows
    public void testSqlFilterConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(ConfigConst.CONCUMER_GROUP);
        consumer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        consumer.subscribe(ConfigConst.TOPIC_NAME, MessageSelector.bySql("a BETWEEN 4 AND 6 AND b = 1"));
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
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        new CountDownLatch(1).await();
    }
}
