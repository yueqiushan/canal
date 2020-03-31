package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.model.UniqueKey;
import com.fanxuankai.canal.model.UniqueKeyPro;

import java.util.List;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public interface RedisUniqueKeyRepository<T> extends RedisReadonly<T> {

    /**
     * 查询
     *
     * @param uniqueKey UniqueKey
     * @return 有可能为empty
     */
    Optional<T> findOne(UniqueKey uniqueKey);

    /**
     * 判断是否存在
     *
     * @param uniqueKey UniqueKey
     * @return true or false
     */
    boolean exists(UniqueKey uniqueKey);

    /**
     * 查询所有
     *
     * @param uniqueKeyPro UniqueKeyPro
     * @return 有可能为empty
     */
    List<T> findAll(UniqueKeyPro uniqueKeyPro);

    /**
     * 查询
     *
     * @param uniqueKey UniqueKey
     * @return 有可能为null
     */
    T getOne(UniqueKey uniqueKey);
}
