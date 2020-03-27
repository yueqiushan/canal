package com.fanxuankai.canal.annotation;

import com.fanxuankai.canal.config.CanalConfigurationSelector;
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
@Import(CanalConfigurationSelector.class)
public @interface EnableCanal {

    /**
     * 应用名, 多实例只会创建一个 canal 连接
     * 因为每个 canal 实例只需要一个客户端, 类似于 c/s 模式
     */
    String name();

    /**
     * 注解扫描的基础包 @CanalEntity
     */
    String[] scanBasePackages();

    /**
     * 开启 redis 缓存
     */
    boolean enableRedis() default true;

    /**
     * 开启消息队列
     */
    boolean enableMq() default false;

    /**
     * 消息队列类型
     */
    MqType mqType() default MqType.RABBIT_MQ;
}
