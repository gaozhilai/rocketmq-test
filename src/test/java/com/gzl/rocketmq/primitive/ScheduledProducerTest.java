package com.gzl.rocketmq.primitive;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

import com.gzl.rocketmq.consts.ConfigConst;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/8/4 11:18
 */
public class ScheduledProducerTest {

    /**
     * 延时消息, 生产的消息会延时给定级别的时间后进行消费
     * 常见用于代替大量的固定时间的定时动作
     * 目前总共支持18个延时级别
     * 在org.apache.rocketmq.store.config.MessageStoreConfig#messageDelayLevel字段进行定义
     * private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
     * 官方给的场景是 比如电商里，提交了一个订单就可以发送一个延时消息，1h后去检查这个订单的状态，如果还是未付款就取消订单释放库存。
     * @throws Exception
     */
    @Test
    public void testScheduledProducer() throws Exception {
        // 实例化一个生产者来产生延时消息
        DefaultMQProducer producer = new DefaultMQProducer(ConfigConst.PRODUCER_GROUP);
        producer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        producer.setSendMsgTimeout(ConfigConst.PRODUCER_TIMEOUT);
        // 启动生产者
        producer.start();
        int totalMessagesToSend = 100;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message(ConfigConst.TOPIC_NAME, "TagA", ("Hello scheduled message " + i).getBytes());
            // 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
            message.setDelayTimeLevel(3);
            // 发送消息
            SendResult send = producer.send(message);
            System.out.println(send);
        }
        Thread.sleep(20000);
        // 关闭生产者
        producer.shutdown();
    }
}
