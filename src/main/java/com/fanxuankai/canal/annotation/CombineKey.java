package com.fanxuankai.canal.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 组合键
 *
 * @author fanxuankai
 */
@Target({})
@Retention(RUNTIME)
public @interface CombineKey {
    String[] values();
}
