package com.fanxuankai.canal.util;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.Resource;

/**
 * 持有 ApplicationContext
 *
 * @author fanxuankai
 */
public class App implements ApplicationContextAware, InitializingBean {
    private static ApplicationContext applicationContext;
    private static RedisTemplate<String, Object> redisTemplate1;
    @Resource
    private RedisTemplate<String, Object> redisTemplate;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        App.applicationContext = applicationContext;
    }

    public static ApplicationContext getContext() {
        return applicationContext;
    }

    public static RedisTemplate<String, Object> getRedisTemplate() {
        return redisTemplate1;
    }

    @Override
    public void afterPropertiesSet() {
        App.redisTemplate1 = redisTemplate;
    }
}
