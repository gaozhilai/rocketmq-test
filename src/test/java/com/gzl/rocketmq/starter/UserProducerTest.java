package com.gzl.rocketmq.starter;

import javax.annotation.Resource;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.Test;

import com.gzl.rocketmq.base.BaseTest;
import com.gzl.rocketmq.consts.ConfigConst;
import com.gzl.rocketmq.entity.User;

import lombok.extern.slf4j.Slf4j;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/8/12 17:58
 */
@Slf4j
public class UserProducerTest extends BaseTest {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Test
    public void testUserProducer() {
        User user = new User().setUserId(1L).setUsername("张三");
        SendResult sendResult = rocketMQTemplate.syncSend(ConfigConst.TOPIC_NAME + ":user_producer", user);
        log.info("发送结果为{}", sendResult);
    }
}
