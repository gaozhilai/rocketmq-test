package com.gzl.rocketmq.primitive;

import java.util.Arrays;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import com.gzl.rocketmq.base.ConfigConst;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/8/5 15:39
 */
@Slf4j
public class FilterProducerTest {

    /**
     * 生产消息时添加额外的用户属性, 用于broker端sql92消息过滤
     */
    @Test
    @SneakyThrows
    public void testSqlFilterProducer() {
        DefaultMQProducer producer = new DefaultMQProducer(ConfigConst.PRODUCER_GROUP);
        producer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        producer.setSendMsgTimeout(ConfigConst.PRODUCER_TIMEOUT);
        producer.start();
        Message msg = new Message(ConfigConst.TOPIC_NAME,
                "TagA",
                ("自定义属性服务器端sql过滤消息, 应该收到").getBytes(RemotingHelper.DEFAULT_CHARSET)
        );
        msg.putUserProperty("a", "5");
        msg.putUserProperty("b", "1");
        Message msgTwo = new Message(ConfigConst.TOPIC_NAME,
                "TagA",
                ("自定义属性服务器端sql过滤消息, 这条不应该收到").getBytes(RemotingHelper.DEFAULT_CHARSET)
        );
        msgTwo.putUserProperty("a", "6");
        msgTwo.putUserProperty("b", "2");
        SendResult send = producer.send(Arrays.asList(msg, msgTwo));
        log.info("发送消息结果为{}", send);
        producer.shutdown();
    }
}
