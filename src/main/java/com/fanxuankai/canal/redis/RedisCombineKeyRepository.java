package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.model.CombineKey;

import java.util.Optional;

/**
 * @author fanxuankai
 */
public interface RedisCombineKeyRepository<T> {

    /**
     * 查询所有
     *
     * @param combineKey List<Ck>
     * @return 有可能为empty
     */
    Optional<T> findOne(CombineKey combineKey);

}
