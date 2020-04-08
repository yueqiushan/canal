package com.fanxuankai.canal.annotation;

import com.fanxuankai.canal.config.CanalConfiguration;
import com.fanxuankai.canal.mq.MqType;
import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author fanxuankai
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Import(CanalConfiguration.class)
public @interface EnableCanal {

    /**
     * 应用名
     * Canal 不支持多客户端, 一个 Canal 实例一个链接
     */
    String name();

    /**
     * 数据库名
     */
    String schema();

    /**
     * 基础包
     */
    String[] scanBasePackages();

    /**
     * 开启 redis 缓存
     */
    boolean enableRedis() default true;

    /**
     * 开启消息队列
     */
    boolean enableMq() default true;

    /**
     * 消息队列类型
     */
    MqType mqType() default MqType.RABBIT_MQ;
}
