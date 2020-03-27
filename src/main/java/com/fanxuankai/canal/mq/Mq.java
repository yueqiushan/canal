package com.fanxuankai.canal.mq;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

/**
 * MQ 消费配置
 *
 * @author fanxuankai
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Mq {

    /**
     * 是否激活
     */
    boolean enable() default false;

    /**
     * 队列名, 默认为 schema.table.EventType, .EventType 后缀自动添加, 不需指定
     */
    String name() default "";

    /**
     * 要消费的事件类型，默认为增、删、改
     */
    CanalEntry.EventType[] eventTypes() default {INSERT, DELETE, UPDATE};

    /**
     * Google Aviator 表达式
     */
    String aviatorExpression() default "";
}
