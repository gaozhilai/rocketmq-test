package com.gzl.rocketmq.primitive;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

import com.gzl.rocketmq.consts.ConfigConst;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/8/5 14:28
 */
@Slf4j
public class BatchProducerTest {

    /**
     * 总数据量小于4MB的消息可以直接批量发送
     */
    @Test
    @SneakyThrows
    public void testBatchProducer() {
        DefaultMQProducer producer = new DefaultMQProducer(ConfigConst.PRODUCER_GROUP);
        producer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        producer.setSendMsgTimeout(ConfigConst.PRODUCER_TIMEOUT);
        producer.start();
        String topic = ConfigConst.TOPIC_NAME;
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "TagA", "OrderID001", "批量消息 0".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID002", "批量消息 1".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID003", "批量消息 2".getBytes()));
        SendResult send = producer.send(messages);
        log.info("消息发送结果为{}", send);
        producer.shutdown();
    }

    /**
     * 如果批量消息数据量大于4MB每次, 那么将消息分割成小于4MB分批批量发送
     */
    @Test
    @SneakyThrows
    public void testBatchProducerMoreThen4MB() {
        DefaultMQProducer producer = new DefaultMQProducer(ConfigConst.PRODUCER_GROUP);
        producer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        producer.setSendMsgTimeout(ConfigConst.PRODUCER_TIMEOUT);
        producer.start();
        String topic = ConfigConst.TOPIC_NAME;
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "TagA", "OrderID001", "批量消息 0".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID002", "批量消息 1".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID003", "批量消息 2".getBytes()));
        // 自定义批量消息分割器
        ListSplitter splitter = new ListSplitter(messages);
        while (splitter.hasNext()) {
            // 将消息分割成小于4MB每批
            List<Message>  listItem = splitter.next();
            // 进行批量发送
            producer.send(listItem);
        }
        producer.shutdown();
    }
}
