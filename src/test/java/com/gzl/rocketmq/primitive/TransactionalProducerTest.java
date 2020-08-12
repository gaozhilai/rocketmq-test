package com.gzl.rocketmq.primitive;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import com.gzl.rocketmq.consts.ConfigConst;

import lombok.SneakyThrows;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/8/5 17:50
 */
public class TransactionalProducerTest {

    /**
     * 在初次接触事务消息的时候感觉事务消息完全可以用本地事务执逻辑之后再发送消息的方式代替, 经过思考发现普通的方式, 不管在本地事务前
     * 发送消息还是本地事务后发送消息, 都有数据不一致的可能性, 而事务消息轻量的解决了这个问题. 经过跟了解RocketMQ比较多的大牛求证, 我的
     * 想法是对的, 在私信求证过程中, 突然发现大牛名字很眼熟, 一看居然是我正在看的<<RocketMQ技术内幕>>书籍作者丁威.
     *
     * RocketMQ4.3开始开源的事务消息部分, 很好的简化了微服务中多服务数据一致性问题
     * 在微服务架构的系统中, 一个业务导致的数据库修改操作可能会跨服务, 每个服务所在数据库甚至都不同
     * 以对数据一致性要求很高的转账操作为例, 假设转账扣款操作与增加余额操作在两个服务里, 那么过程如下
     * 服务1 主表扣款, 明细表增加对账记录
     * 服务2 主表增加余额, 明细表增加对账记录
     * 其中同一个服务内的主表与明细表操作, 可以放在一个事务中确保数据操作的原子性, 两个表共同成功共同失败
     *
     * 但是服务1操作, 服务2操作无法通过放在同一个本地事务, 无法保证服务1成功, 服务2也成功, 假设过程如下
     *
     * 服务1 本地扣款成功, 调用服务2
     * 服务2 增加余额成功
     * 这种方式所有与服务1场景相似的服务, 都要重复增加重试机制, 防止调用不成功, 如果数据库操作成功, 服务挂了调用失败, 最终数据不一致
     *
     * 服务1 调用服务2, 本地扣款成功
     * 服务2 增加余额成功
     * 这种顺序看起来不靠谱, 实际也不靠谱. 比如调用服务2成功了, 本地数据库操作失败本地事务回滚, 最终造成数据不一致
     *
     *
     * 这时我们引入消息队列, 尝试增加一些可怜的可靠性.
     * 引入消息队列的好处是只要消息发送成功了, MQ的高可靠性会让消息一定至少被消费一次, 并且消息能被持久化一定时间, 这时过程如下
     *
     * 服务1 发送消息, 本地扣款成功
     * 服务2 消费消息, 进行余额增加操作
     * 这种过程缺陷明显, 服务发送消息后, 数据库操作失败, 此时消息却无法撤回, 造成数据不一致
     *
     *
     * 服务1 本地扣款操作成功, 发送消息
     * 服务2 消费消息, 进行余额增加操作
     * 这种过程看起来好一些, 但是也是不可靠的, 比如服务1扣款成功, 节点挂掉, 消息发送失败, 造成数据不一致
     *
     * 上面我们罗列了原始情况和引入消息队列后的情况都不能保证数据一致性以及其原因. 从上面的内容可以看到, 数据不一致的原因总结来说
     * 是因为
     * ①服务调用服务/发送消息成功, 但是本地事务失败回滚
     * ②本地事务成功, 但是调用服务/发送消息失败
     *
     * RocketMQ事务消息很好很简洁的解决了这一问题, 此时所有服务不需要自己考虑可靠性问题, 可靠性由微服务体系与消息队列保证, 服务自身只需要处理好
     * 消费消息的幂等性防止重复消费即可(最简单的方式要处理的数据库表增加状态字段标识是否处理过此类型消息), 事务消息的原理也很简单, 过程如下
     * RocketMQ引入消息的中间状态(half message), 通过两阶段提交(提交消息内容, 提交消息状态)来保证消息的事务性
     * 第一阶段是发送消息到broker, 此时消息对消费者还不可见
     * 第二阶段是检查本地事务是否成功, 并告知broker当前事务消息的状态
     *
     * 反馈的事务消息状态分为三种:
     * UNKNOW 需要重试检查本地事务状态, 重试一定次数还是此状态时, 此事务消息被丢弃
     * COMMIT_MESSAGE 本地事务成功, 提交消息, 此时事务消息能正常被消费到
     * ROLLBACK_MESSAGE 本地事务失败, 回滚消息, 此时事务消息被丢弃
     *
     * 引入事务消息能保证数据一致性的原因的本质是
     * ①在执行本地事务前发送事务消息, 此时消息发送不成功直接反馈错误, 发送成功才尝试执行本地事务, 虽然发送成功, 但是half message无法被消费者感知到
     * ②执行本地事务
     * ③检测本地事务执行结果, 成功反馈给broker提交消息, 消息可以被消费到. 失败反馈给broker回滚消息, 消息被丢弃不会被消费, 此时保证了本地事务与消息是一个共同成功/失败的操作
     * 额外说明:
     * 即使本地事务执行成功后服务挂掉, 无法反馈事务执行结果也没关系, broker会从同一生产者组选一个节点尝试对消息进行提交
     * 即使消费前服务2一个节点挂掉也没关系, 在集群消费模式下, 同一消费者组其他某个节点会将消息消费掉
     *
     *
     * 事务消息的限制与相关自定义配置
     * ①事务消息不支持延时消息和批量消息(不支持批量消息这个要格外注意)
     * ②单个UNKNOW状态消息, 检测本地事务执行结果的默认次数为15次, 超过即被丢弃. 可以通过broker配置transactionCheckMax更改
     * ③可以通过修改broker配置transactionTimeout来控制broker经过多久后检查本地事务执行结果
     * ④事务消息不能与非事务消息生产者处于同一个生产者组, 这个其实不需要额外注意, 生产者组本身就是要生产逻辑一致的生产者构成
     *
     */
    @Test
    @SneakyThrows
    public void testTansactionalProducer() {
        TransactionListener transactionListener = new TransactionListenerImpl();
        TransactionMQProducer producer = new TransactionMQProducer(ConfigConst.PRODUCER_GROUP);
        producer.setNamesrvAddr(ConfigConst.NAMESVR_ADDR);
        producer.setSendMsgTimeout(ConfigConst.PRODUCER_TIMEOUT);
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        producer.setExecutorService(executorService);
        producer.setTransactionListener(transactionListener);
        producer.start();
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            try {
                Message msg =
                        new Message(ConfigConst.TOPIC_NAME, tags[i % tags.length], "KEY" + i,
                                ("测试事务消息 " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("%s%n", sendResult);
                Thread.sleep(10);
            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }

    /**
     * 事务消息自己实现的事务监听器, 用于根据本地事务结果反馈消息状态
     */
    private class TransactionListenerImpl implements TransactionListener {
        private AtomicInteger transactionIndex = new AtomicInteger(0);
        private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

        /**
         * 发送中间状态消息(prepare(half) message)后就会执行此方法, 检查本地事务是否成功
         * @param msg 发送的中间状态消息(prepare(half) message)
         * @param arg 用户自定义的业务参数
         * @return 消息状态(UNKNOW继续检查, ROLLBACK_MESSAGE回滚消息, COMMIT_MESSAGE提交消息)
         */
        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            int value = transactionIndex.getAndIncrement();
            int status = value % 3;
            localTrans.put(msg.getTransactionId(), status);
            // 本地事务执行完毕后返回UNKONW, 测试broker主动回调查询事务执行结果的情况
            return LocalTransactionState.UNKNOW;
        }

        /**
         * 当本地事务方法检查后返回UNKNOW时, 或者方法无返回结果比如系统宕机, broker方法会调用此方法
         * 当发送者宕机导致的执行本地事务方法没有返回结果时, broker会在同一生产者组节点中寻求执行此方法
         * @param msg 发送的中间状态消息(prepare(half) message)
         * @return 消息状态(UNKNOW继续检查, ROLLBACK_MESSAGE回滚消息, COMMIT_MESSAGE提交消息)
         */
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            Integer status = localTrans.get(msg.getTransactionId());
            // 只有消息对应的transactionIndex % 3结果为1的消息会返回COMMIT_MESSAGE, 消息被提交可以被消费到
            // 结果为0的返回UNKNOW状态, 再重新检查一定次数后最终会被删除
            // 结果为2的消息会返回ROLLBACK_MESSAGE立刻被删除
            if (null != status) {
                switch (status) {
                    case 0:
                        return LocalTransactionState.UNKNOW;
                    case 1:
                        return LocalTransactionState.COMMIT_MESSAGE;
                    case 2:
                        return LocalTransactionState.ROLLBACK_MESSAGE;
                }
            }
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}
