package com.fanxuankai.canal.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.constants.CommonConstants;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.model.CombineKey;
import com.fanxuankai.canal.model.Entry;
import com.fanxuankai.canal.model.UniqueKey;
import com.fanxuankai.canal.model.UniqueKeyPro;
import com.fanxuankai.canal.util.RedisUtils;
import com.google.common.collect.Sets;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class DefaultRedisRepository<T> implements RedisReadonly<T>, RedisUniqueKeyRepository<T>,
        RedisCombineKeyRepository<T> {

    @Resource
    protected RedisTemplate<Object, Object> redisTemplate;
    protected Class<T> tClass;
    private CanalEntityMetadata metadata;

    @SuppressWarnings("unchecked")
    public DefaultRedisRepository() {
        tClass =
                (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.metadata = CanalEntityMetadataCache.getMetadata(tClass);
    }

    @Override
    public Optional<T> findById(Object id) {
        return Optional.ofNullable(getOne(id));
    }

    @Override
    public boolean existsById(Object id) {
        return getOne(id) != null;
    }

    @Override
    public List<T> findAll() {
        return getAll(RedisUtils.key(metadata.getTableMetadata().getSchema(), tableName()));
    }

    @Override
    public List<T> findAllById(Iterable<Object> ids) {
        if (ids == null) {
            return Collections.emptyList();
        }
        Set<Object> idSet = Sets.newHashSet();
        for (Object id : ids) {
            idSet.add(id.toString());
        }
        return multiGet(RedisUtils.key(metadata.getTableMetadata().getSchema(), tableName()), idSet);
    }

    @Override
    public long count() {
        return redisTemplate.opsForHash().size(RedisUtils.key(metadata.getTableMetadata().getSchema(),
                tableName()));
    }

    @Override
    public T getOne(Object id) {
        return convert(redisTemplate.opsForHash().get(RedisUtils.key(metadata.getTableMetadata().getSchema(),
                tableName()), id.toString()));
    }

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

    protected List<T> getAll(String key) {
        return convert(redisTemplate.opsForHash().values(key));
    }

    protected List<T> multiGet(String key, Collection<Object> hashKeys) {
        return convert(redisTemplate.opsForHash().multiGet(key, hashKeys));
    }

    protected List<T> convert(List<Object> objects) {
        objects = objects.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(objects)) {
            return Collections.emptyList();
        }
        return new JSONArray(objects).toJavaList(tClass);
    }

    protected T convert(Object o) {
        if (o == null) {
            return null;
        }
        return JSON.parseObject(JSON.toJSONString(o), tClass);
    }

    protected String schema() {
        return metadata.getTableMetadata().getSchema();
    }

    protected String tableName() {
        return metadata.getTableMetadata().getName();
    }
}
