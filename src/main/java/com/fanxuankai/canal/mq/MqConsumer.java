package com.fanxuankai.canal.mq;

/**
 * MQ 消费者接口
 *
 * @author fanxuankai
 */
public interface MqConsumer<T> {

    /**
     * 增
     *
     * @param t 新增的数据
     */
    default void insert(T t) {

    }

    /**
     * 改
     *
     * @param before 修改前数据
     * @param after  修改后数据
     */
    default void update(T before, T after) {

    }

    /**
     * 删
     *
     * @param t 删除的数据
     */
    default void delete(T t) {

    }

}
