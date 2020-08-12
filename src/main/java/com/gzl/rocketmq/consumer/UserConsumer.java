package com.gzl.rocketmq.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

import com.gzl.rocketmq.consts.ConfigConst;
import com.gzl.rocketmq.entity.User;

import lombok.extern.slf4j.Slf4j;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/8/12 17:53
 */
@Service
@RocketMQMessageListener(topic = ConfigConst.TOPIC_NAME, consumerGroup = "user_consumer",
        selectorType = SelectorType.TAG, selectorExpression = "user_producer")
@Slf4j
public class UserConsumer implements RocketMQListener<User> {

    @Override
    public void onMessage(User message) {
        log.info("已对象的形式接收到消息{}", message);
    }
}
