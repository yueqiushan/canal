package com.fanxuankai.canal.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.fanxuankai.canal.constants.CommonConstants;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.CanalEntityMetadataCache;
import com.fanxuankai.canal.model.CombineKey;
import com.fanxuankai.canal.model.Entry;
import com.fanxuankai.canal.model.UniqueKey;
import com.fanxuankai.canal.model.UniqueKeyPro;
import com.fanxuankai.canal.util.App;
import com.fanxuankai.canal.util.RedisUtils;
import com.google.common.collect.Sets;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * RedisRepository 实现类
 *
 * @author fanxuankai
 */
public class SimpleRedisRepository implements RedisRepository<Object> {

    protected RedisTemplate<String, Object> redisTemplate;
    protected Class<Object> domainType;
    private CanalEntityMetadata metadata;

    /**
     * SimpleRedisRepository 合理的话, 应该使用泛型
     * 由于 Javassist 不支持泛型, 暂且使用设置 domainType 的方式
     *
     * @param domainType 实体类型
     */
    protected void setDomainType(Class<Object> domainType) {
        this.domainType = domainType;
        this.metadata = CanalEntityMetadataCache.getMetadata(domainType);
        this.redisTemplate = App.getRedisTemplate();
    }

    // Javassist 不支持泛型
//    @SuppressWarnings("unchecked")
//    public SimpleRedisRepository() {
//        tClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
//        this.metadata = CanalEntityMetadataCache.getMetadata(tClass);
//    }

    @Override
    public Optional<Object> findById(Object id) {
        return Optional.ofNullable(getOne(id));
    }

    @Override
    public boolean existsById(Object id) {
        return getOne(id) != null;
    }

    @Override
    public List<Object> findAll() {
        return getAll(RedisUtils.key(metadata.getTableMetadata().getSchema(), tableName()));
    }

    @Override
    public List<Object> findAllById(Iterable<Object> ids) {
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
    public Object getOne(Object id) {
        return convert(redisTemplate.opsForHash().get(RedisUtils.key(metadata.getTableMetadata().getSchema(),
                tableName()), id.toString()));
    }

    @Override
    public Optional<Object> findOne(UniqueKey uniqueKey) {
        return Optional.ofNullable(getOne(uniqueKey));
    }

    @Override
    public boolean exists(UniqueKey uniqueKey) {
        return findOne(uniqueKey).isPresent();
    }

    @Override
    public Optional<Object> findOne(CombineKey combineKey) {
        List<Entry> entries = combineKey.getEntries().stream().distinct().collect(Collectors.toList());
        String suffix =
                entries.stream().map(Entry::getName).collect(Collectors.joining(CommonConstants.SEPARATOR1));
        String key = RedisUtils.key(schema(), tableName(), suffix);
        String hashKey =
                entries.stream().map(Entry::getValue).map(Object::toString).collect(Collectors.joining(CommonConstants.SEPARATOR1));
        return Optional.ofNullable(convert(redisTemplate.opsForHash().get(key, hashKey)));
    }

    @Override
    public List<Object> findAll(UniqueKeyPro uniqueKeyPro) {
        String key = RedisUtils.key(schema(), tableName(), uniqueKeyPro.getName());
        Set<Object> hashKeys = new HashSet<>();
        for (Object value : uniqueKeyPro.getValues()) {
            hashKeys.add(value.toString());
        }
        return multiGet(key, hashKeys);
    }

    @Override
    public Object getOne(UniqueKey uniqueKey) {
        return convert(redisTemplate.opsForHash().get(RedisUtils.key(schema(),
                tableName(), uniqueKey.getName()), uniqueKey.getValue().toString()));
    }

    protected List<Object> getAll(String key) {
        return convert(redisTemplate.opsForHash().values(key));
    }

    protected List<Object> multiGet(String key, Collection<Object> hashKeys) {
        return convert(redisTemplate.opsForHash().multiGet(key, hashKeys));
    }

    protected List<Object> convert(List<Object> objects) {
        objects = objects.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(objects)) {
            return Collections.emptyList();
        }
        return new JSONArray(objects).toJavaList(domainType);
    }

    protected Object convert(Object o) {
        if (o == null) {
            return null;
        }
        return JSON.parseObject(JSON.toJSONString(o), domainType);
    }

    protected String schema() {
        return metadata.getTableMetadata().getSchema();
    }

    protected String tableName() {
        return metadata.getTableMetadata().getName();
    }
}
