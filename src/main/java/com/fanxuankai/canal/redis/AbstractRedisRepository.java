package com.fanxuankai.canal.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
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
public abstract class AbstractRedisRepository<T> implements RedisReadonly<T> {

    @Resource
    protected RedisTemplate<Object, Object> redisTemplate;
    protected Class<T> tClass;
    private CanalEntityMetadata metadata;

    @SuppressWarnings("unchecked")
    public AbstractRedisRepository() {
        tClass =
                (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[typeIndex()];
        this.metadata = CanalEntityMetadataCache.getMetadata(tClass);
    }

    /**
     * 泛型索引
     */
    protected abstract int typeIndex();

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
