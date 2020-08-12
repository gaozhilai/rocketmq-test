package com.gzl.rocketmq.consts;

import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;

import lombok.experimental.UtilityClass;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/8/5 15:00
 */
@UtilityClass
public class ConfigConst {

    /**
     * 名字服务器地址
     */
    public static final String NAMESVR_ADDR = "192.168.128.129:9876";

    /**
     * 生产/订阅消息的Topic名称
     */
    public static final String TOPIC_NAME = "TopicTest";

    /**
     * 消费者组名称, 无参构造消费者会报错 consumerGroup can not equal DEFAULT_CONSUMER, please specify another one
     */
    public static final String CONCUMER_GROUP = "ConsumerTestGroup";

    /**
     * 生产者组名称, 无参构造生产者会报错 producerGroup can not equal DEFAULT_PRODUCER, please specify another one.
     */
    public static final String PRODUCER_GROUP = "ProducerTestGroup";

    /**
     * 生产者超时时间, 初始值15秒, 防止本机网卡配置低导致的超时异常{@link RemotingTooMuchRequestException}
     */
    public static final int PRODUCER_TIMEOUT = 15_000;
}
