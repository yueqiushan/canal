package com.fanxuankai.canal.flow;

import com.fanxuankai.canal.config.CanalConfig;
import lombok.Builder;
import lombok.Data;

/**
 * @author fanxuankai
 */
@Data
@Builder
public class ConnectConfig {
    /**
     * canal 配置
     */
    private CanalConfig canalConfig;
    /**
     * canal 实例名
     */
    private String instance;
    /**
     * 订阅表达式
     */
    private String filter;

    private String subscriberName;
}
