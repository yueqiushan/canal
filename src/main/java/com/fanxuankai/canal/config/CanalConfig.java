package com.fanxuankai.canal.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * canal参数配置
 *
 * @author fanxuankai
 */
@Configuration
@ConfigurationProperties(prefix = "canal")
@Data
public class CanalConfig {

    /**
     * 集群配置
     */
    private Cluster cluster;

    /**
     * 单节点配置
     */
    private SingleNode singleNode = new SingleNode();

    /**
     * 账号
     */
    private String username = "canal";

    /**
     * 密码
     */
    private String password = "canal";

    /**
     * redis 对应的 canal 实例名
     */
    private String redisInstance = "example";

    /**
     * mq 对应的 canal 实例名
     */
    private String mqInstance = "example";

    /**
     * 数据库
     */
    private String schema;

    /**
     * 拉取数据的间隔(ms)
     */
    private long intervalMillis = 1000;

    /**
     * 拉取数据的数量
     */
    private int batchSize = 100;

    /**
     * 打印日志
     */
    private boolean showLog;

    /**
     * 显示数据变动的日志
     */
    private boolean showRowChange;

    /**
     * 美化数据变动的日志
     */
    private boolean formatRowChangeLog;

    @Data
    public static class Cluster {
        /**
         * zookeeper host:port
         */
        private String nodes = "localhost:2181,localhost:2182,localhost:2183";
    }

    @Data
    public static class SingleNode {
        /**
         * host
         */
        private String hostname = "localhost";
        /**
         * port
         */
        private int port = 11111;
    }

}
