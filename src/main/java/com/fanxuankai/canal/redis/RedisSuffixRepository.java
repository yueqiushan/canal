package com.fanxuankai.canal.redis;

import java.util.List;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public interface RedisSuffixRepository<T> extends RedisReadonly<T> {

    /**
     * 查询
     *
     * @param suffix 后缀
     * @param value  值
     * @return 有可能为empty
     */
    Optional<T> findBySuffix(String suffix, Object value);

    /**
     * 判断是否存在
     *
     * @param suffix 后缀
     * @param value  值
     * @return true or false
     */
    boolean existsBySuffix(String suffix, Object value);

    /**
     * 查询所有
     *
     * @param suffix 后缀
     * @param values 值
     * @return 有可能为empty
     */
    List<T> findAllBySuffix(String suffix, Iterable<Object> values);

    /**
     * 查询
     *
     * @param suffix 后缀
     * @param value  主键
     * @return 有可能为null
     */
    T getOne(String suffix, Object value);
}
