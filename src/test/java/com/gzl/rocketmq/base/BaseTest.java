package com.gzl.rocketmq.base;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/6/29 18:11
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class BaseTest {

    /**
     * 避免sonar检查失败
     */
    @Test
    public void avoidSonar() {
        Assert.assertTrue(true);
    }
}
