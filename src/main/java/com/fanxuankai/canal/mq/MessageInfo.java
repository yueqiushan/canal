package com.fanxuankai.canal.mq;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

/**
 * 消息实体
 *
 * @author fanxuankai
 */
@AllArgsConstructor
@Getter
public class MessageInfo {
    /**
     * 消息主题
     */
    private String routingKey;
    /**
     * 消息集合
     */
    private List<String> messages;
}