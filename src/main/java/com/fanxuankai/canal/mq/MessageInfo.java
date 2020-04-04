package com.fanxuankai.canal.mq;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

/**
 * @author fanxuankai
 */
@AllArgsConstructor
@Getter
public class MessageInfo {
    private String routingKey;
    private List<String> messages;
}