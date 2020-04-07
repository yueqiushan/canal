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
     * 数据库名, 默认为 @EnableCanal.schema()
     */
    String schema() default "";

    /**
     * 数据库表名
     * 如果使用了 javax.persistence.Table 注解, 取 @Table.value(), 否则取实体类名转下划线
     */
    String name() default "";
}
