package com.fanxuankai.canal.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 过滤
 *
 * @author fanxuankai
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Filter {
    /**
     * Google Aviator boolean 表达式, 表达式必须返回 true or false
     */
    String aviatorExpression() default "";

    /**
     * 过滤已更新的字段
     * 指定了字段则需要满足字段的值是有修改
     */
    String[] updatedFields() default {};
}
