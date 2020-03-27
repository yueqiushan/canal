package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.annotation.CombineKey;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Redis 消费配置
 *
 * @author fanxuankai
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Redis {

    /**
     * 是否激活
     */
    boolean enable() default true;

    /**
     * hash key, 默认为 schema.table
     */
    String key() default "";

    /**
     * id 作为 hash 的 field
     */
    boolean idAsField() default true;

    /**
     * hash key 增加 uniqueKey 后缀, 作为 hash 的集合名, 以 uniqueKey 的值作为 hash 的 field
     */
    String[] uniqueKeys() default {};

    /**
     * hash key 增加 combineKeys 后缀, 作为 hash 的集合名, 以 combineKeys 的值作为 hash 的 field
     */
    CombineKey[] combineKeys() default {};

    /**
     * Google Aviator 表达式
     */
    String aviatorExpression() default "";

}
