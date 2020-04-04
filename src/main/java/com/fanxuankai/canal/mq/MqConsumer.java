package com.fanxuankai.canal.mq;

/**
 * 消息消费者
 *
 * @author fanxuankai
 */
public interface MqConsumer<T> {
    /**
     * 增
     *
     * @param entity 新增的数据
     */
    void insert(T entity);

    /**
     * 改
     *
     * @param oldEntity 修改前数据
     * @param newEntity 修改后数据
     */
    void update(T oldEntity, T newEntity);

    /**
     * 删
     *
     * @param entity 删除的数据
     */
    void delete(T entity);

}
