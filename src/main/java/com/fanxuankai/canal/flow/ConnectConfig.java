package com.fanxuankai.canal.flow;

import lombok.Builder;
import lombok.Getter;

/**
 * Canal 连接配置文件
 *
 * @author fanxuankai
 */
@Getter
@Builder
public class ConnectConfig {
    /**
     * canal 实例名
     */
    private String instance;

    /**
     * 订阅表达式
     */
    private String filter;

    /**
     * 订阅者
     */
    private String subscriberName;
}
