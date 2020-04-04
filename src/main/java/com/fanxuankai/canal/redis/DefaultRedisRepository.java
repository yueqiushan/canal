package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.constants.CommonConstants;
import com.fanxuankai.canal.model.CombineKey;
import com.fanxuankai.canal.model.Entry;
import com.fanxuankai.canal.model.UniqueKey;
import com.fanxuankai.canal.model.UniqueKeyPro;
import com.fanxuankai.canal.util.RedisUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
@Slf4j
public class DefaultRedisRepository<T> extends AbstractRedisRepository<T> implements RedisUniqueKeyRepository<T>,
        RedisCombineKeyRepository<T> {

    @Override
    public Optional<T> findOne(UniqueKey uniqueKey) {
        return Optional.ofNullable(getOne(uniqueKey));
    }

    @Override
    public boolean exists(UniqueKey uniqueKey) {
        return findOne(uniqueKey).isPresent();
    }

    @Override
    public Optional<T> findOne(CombineKey combineKey) {
        List<Entry> entries = combineKey.getEntries().stream().distinct().collect(Collectors.toList());
        String suffix =
                entries.stream().map(Entry::getName).collect(Collectors.joining(CommonConstants.SEPARATOR1));
        String key = RedisUtils.key(schema(), tableName(), suffix);
        String hashKey =
                entries.stream().map(Entry::getValue).map(Object::toString).collect(Collectors.joining(CommonConstants.SEPARATOR1));
        return Optional.ofNullable(convert(redisTemplate.opsForHash().get(key, hashKey)));
    }

    @Override
    public List<T> findAll(UniqueKeyPro uniqueKeyPro) {
        String key = RedisUtils.key(schema(), tableName(), uniqueKeyPro.getName());
        Set<Object> hashKeys = new HashSet<>();
        for (Object value : uniqueKeyPro.getValues()) {
            hashKeys.add(value.toString());
        }
        return multiGet(key, hashKeys);
    }

    @Override
    public T getOne(UniqueKey uniqueKey) {
        return convert(redisTemplate.opsForHash().get(RedisUtils.key(schema(),
                tableName(), uniqueKey.getName()), uniqueKey.getValue().toString()));
    }

    @Override
    protected int typeIndex() {
        return 0;
    }
}
