package com.gzl.rocketmq.primitive;

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
 * @date 2020/8/4 11:15
 */
public class ScheduledConsumerTest {

    /**
     * 消费者, 打印了消息的broker存储时间与消费时间, 可以看到消费消息的时间的确按照预设延时了
     */
    @Test
    @SneakyThrows
    public void testScheduledConsumer() {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(ConfigConst.CONCUMER_GROUP);
        consumer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        // 订阅Topics
        consumer.subscribe(ConfigConst.TOPIC_NAME, "*");
        // 注册消息监听者
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
                for (MessageExt message : messages) {
                    // Print approximate delay time period
                    System.out.println("Receive message[msgId=" + message.getMsgId() + "] " + (System.currentTimeMillis() - message.getStoreTimestamp()) + "ms later");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        new CountDownLatch(1).await();
    }
}
