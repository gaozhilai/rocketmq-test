package com.gzl.rocketmq.primitive;

import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.joda.time.DateTime;
import org.junit.Test;

import com.gzl.rocketmq.consts.ConfigConst;

import lombok.Data;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/7/31 14:09
 */
public class OrderedProducerTest {

    /**
     * 此示例为同一分区队列的情况下保证顺序生产, 即分区有序
     * 通过同一id把需要保持顺序的消息发送到同一个队列, 就保证了当前业务的消息有序
     * 如果想要全局有序需要将分区队列数量设置为1
     */
    @Test
    @SneakyThrows
    public void testOrderedProducer() {
        // 获取到订单, 每个订单由有序的步骤组成
        List<OrderStep> orders = buildOrders();
        DefaultMQProducer producer = new DefaultMQProducer(ConfigConst.PRODUCER_GROUP);
        producer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        producer.setSendMsgTimeout(ConfigConst.PRODUCER_TIMEOUT);
        producer.start();
        for (int i = 0; i < orders.size(); i++) {
            String body = new DateTime().toString("yyyy-MM-dd HH:mm:ss") + orders.get(i);
            Message message = new Message(ConfigConst.TOPIC_NAME, "TagA", "KEY" + i,
                    body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(message, (messageQueues, msg, arg) -> {
                long id = (long) arg;
                long index = id % messageQueues.size();
                return messageQueues.get((int) index);
            }, orders.get(i).getOrderId());
            System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
                    sendResult.getSendStatus(),
                    sendResult.getMessageQueue().getQueueId(),
                    body));
        }
        producer.shutdown();
    }

    @Data
    @Accessors(chain = true)
    private static class OrderStep{

        /**
         * 订单id, 多个订单步骤拥有相同的订单id
         */
        private Long orderId;
        /**
         * 订单步骤描述
         */
        private String desc;

        // 省略getter setter
    }

    private List<OrderStep> buildOrders() {
        OrderStep orderOneStep1 = new OrderStep().setOrderId(1L).setDesc("id为1的订单第一步");
        OrderStep orderOneStep2 = new OrderStep().setOrderId(1L).setDesc("id为1的订单第二步");
        OrderStep orderOneStep3 = new OrderStep().setOrderId(1L).setDesc("id为1的订单第三步");
        OrderStep orderTwoStep1 = new OrderStep().setOrderId(2L).setDesc("id为2的订单第一步");
        OrderStep orderTwoStep2 = new OrderStep().setOrderId(2L).setDesc("id为2的订单第二步");
        OrderStep orderTwoStep3 = new OrderStep().setOrderId(2L).setDesc("id为2的订单第三步");
        return Arrays.asList(orderOneStep1, orderOneStep2, orderOneStep3, orderTwoStep1, orderTwoStep2, orderTwoStep3);
    }
}
