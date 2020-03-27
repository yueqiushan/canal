package com.fanxuankai.canal.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author fanxuankai
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    /**
     * 数据库名, 默认为 canal 配置的 schema
     */
    String schema() default "";

    /**
     * 数据库表名, 默认为 javax.persistence.Table | 实体类名转下划线
     */
    String name() default "";
}
