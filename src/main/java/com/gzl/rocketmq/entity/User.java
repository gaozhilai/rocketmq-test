package com.gzl.rocketmq.entity;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * No Description
 *
 * @author GaoZhilai
 * @date 2020/8/12 17:50
 */
@Data
@Accessors(chain = true)
public class User {

    /**
     * 用户id
     */
    private Long userId;

    /**
     * 用户姓名
     */
    private String username;
}
