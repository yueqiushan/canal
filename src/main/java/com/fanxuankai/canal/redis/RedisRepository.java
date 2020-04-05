package com.fanxuankai.canal.redis;

import java.util.List;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public interface RedisRepository<T> extends RedisUniqueKeyRepository<T>, RedisCombineKeyRepository<T> {
    /**
     * 指定id查询
     *
     * @param id 主键
     * @return 有可能为empty
     */
    Optional<T> findById(Object id);

    /**
     * 指定id判断是否存在
     *
     * @param id 主键
     * @return true or false
     */
    boolean existsById(Object id);

    /**
     * 查询所有
     *
     * @return list
     */
    List<T> findAll();

    /**
     * 指定id查询
     *
     * @param ids 主键
     * @return 有可能为empty
     */
    List<T> findAllById(Iterable<Object> ids);

    /**
     * 判断数量
     *
     * @return long
     */
    long count();

    /**
     * 指定id查询
     *
     * @param id 主键
     * @return 有可能为null
     */
    T getOne(Object id);
}
