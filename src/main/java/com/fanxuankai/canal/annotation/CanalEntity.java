package com.fanxuankai.canal.annotation;

import com.fanxuankai.canal.mq.Mq;
import com.fanxuankai.canal.redis.Redis;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author fanxuankai
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface CanalEntity {
    Table table() default @Table;

    Redis redis() default @Redis;

    Mq mq() default @Mq;
}
