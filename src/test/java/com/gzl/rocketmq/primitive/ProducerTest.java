package com.gzl.rocketmq.primitive;

import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import com.gzl.rocketmq.base.ConfigConst;

import lombok.SneakyThrows;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/7/31 10:30
 */
public class ProducerTest {

    /**
     * 普通方式同步的生产发送消息
     */
    @Test
    @SneakyThrows
    public void testSyncProducer() {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer(ConfigConst.PRODUCER_GROUP);
        // 设置NameServer的地址
        producer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        producer.setSendMsgTimeout(5000);
        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 1; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }

    /**
     * 异步向broker发送消息, 发起发送消息请求时立刻返回执行下一条语句, 用于生产者不能长时间等待发送结果的场景
     * 当异步消息发送成功或异常时会调用相应的回调方法
     */
    @Test
    @SneakyThrows
    public void testAsyncProducer() {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer(ConfigConst.PRODUCER_GROUP);
        // 设置NameServer的地址
        producer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        producer.setSendMsgTimeout(ConfigConst.PRODUCER_TIMEOUT);
        // 启动Producer实例
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int messageCount = 1;
        // 根据消息数量实例化倒计时计算器
        final CountDownLatch2 countDownLatch = new CountDownLatch2(messageCount);
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message(ConfigConst.TOPIC_NAME,
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            // SendCallback接收异步返回结果的回调
            producer.send(msg, new SendCallback() {
                /**
                 * 异步发送消息成功回调
                 * @param sendResult 发送结果
                 */
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                /**
                 * 异步发送消息失败回调
                 * @param e 异常
                 */
                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }
        // 等待5s, 等待异步发送消息完成再继续执行主线程, 关闭生产者
        countDownLatch.await(5, TimeUnit.SECONDS);
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }

    /**
     * 发送单向消息, 不返回发送结果, 常用于发送日志等对发送结果要求不高的场景
     */
    @Test
    @SneakyThrows
    public void testOnewayProducer() {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer(ConfigConst.PRODUCER_GROUP);
        // 设置NameServer的地址
        producer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        producer.setSendMsgTimeout(ConfigConst.PRODUCER_TIMEOUT);
        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 1; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message(ConfigConst.TOPIC_NAME /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 发送单向消息，没有任何返回结果
            producer.sendOneway(msg);

        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
