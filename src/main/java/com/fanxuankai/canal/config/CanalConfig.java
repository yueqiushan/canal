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
    private Long intervalMillis = 1000L;

    /**
     * 拉取数据的数量
     */
    private Integer batchSize = 100;

    /**
     * 打印日志
     */
    private Boolean showLog = Boolean.FALSE;

    /**
     * 显示数据变动的日志
     */
    private Boolean showRowChange = Boolean.FALSE;

    /**
     * 美化数据变动的日志
     */
    private Boolean formatRowChangeLog = Boolean.FALSE;

    /**
     * 开启 Redis 缓存, 如果设置该值会覆盖 EnableCanal 注解
     */
    private Boolean enableRedis;

    /**
     * 开启 MQ, 如果设置该值会覆盖 EnableCanal 注解
     */
    private Boolean enableMq;

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
