package com.fanxuankai.canal.redis;

import com.alibaba.fastjson.JSON;
import com.fanxuankai.canal.annotation.CanalEntity;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.metadata.RedisMetadata;
import com.fanxuankai.canal.util.RedisUtil;
import com.fanxuankai.canal.util.ReflectionUtils;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
@Slf4j
public class RedisRepository<T> implements RedisSuffixRepository<T> {
    @Resource
    protected RedisTemplate<Object, Object> redisTemplate;
    @Resource
    private CanalConfig canalConfig;
    private Class<T> tClass;
    private RedisMetadata redisMetadata;

    @SuppressWarnings("unchecked")
    public RedisRepository() {
        Class<?> aClass = this.getClass();
        List<TypeInformation<?>> arguments =
                ClassTypeInformation.from(aClass).getRequiredSuperTypeInformation(RedisRepository.class).getTypeArguments();
        this.tClass = (Class<T>) resolveTypeParameter(arguments, () -> String.format("Could not resolve domain " +
                "type of %s!", aClass));
        RedisMetadata redisMetadata = CanalEntityMetadataCache.getRedisMetadata(tClass);
        if (redisMetadata == null || !redisMetadata.isEnable()) {
            throw new RuntimeException(String.format("请使用注解(%s)且开启redis: %s", CanalEntity.class, aClass));
        }
        this.redisMetadata = redisMetadata;
    }

    private static Class<?> resolveTypeParameter(List<TypeInformation<?>> arguments,
                                                 Supplier<String> exceptionMessage) {
        if (arguments.size() > 0 && arguments.get(0) != null) {
            return ((TypeInformation<?>) arguments.get(0)).getType();
        } else {
            throw new IllegalArgumentException(exceptionMessage.get());
        }
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
        List<Object> values = redisTemplate.opsForHash().values(RedisUtil.key(canalConfig.getSchema(),
                tableName()));
        if (CollectionUtils.isEmpty(values)) {
            return Collections.emptyList();
        }
        return values.stream()
                .filter(Objects::nonNull)
                .map(o -> JSON.parseObject(o.toString(), tClass))
                .collect(Collectors.toList());
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
        List<Object> objects = multiGet(RedisUtil.key(canalConfig.getSchema(), tableName()), idSet);
        if (CollectionUtils.isEmpty(objects)) {
            return Collections.emptyList();
        }
        return objects.stream()
                .filter(Objects::nonNull)
                .map(o -> JSON.parseObject(o.toString(), tClass))
                .collect(Collectors.toList());
    }

    @Override
    public long count() {
        return redisTemplate.opsForHash().size(RedisUtil.key(canalConfig.getSchema(),
                tableName()));
    }

    @Override
    public T getOne(Object id) {
        Object o = redisTemplate.opsForHash().get(RedisUtil.key(canalConfig.getSchema(),
                tableName()), id.toString());
        if (o == null) {
            return null;
        }
        return JSON.parseObject(o.toString(), tClass);
    }

    @Override
    public Optional<T> findBySuffix(String suffix, Object value) {
        return Optional.ofNullable(getOne(suffix, value));
    }

    @Override
    public boolean existsBySuffix(String suffix, Object value) {
        return getOne(suffix, value) != null;
    }

    @Override
    public List<T> findAllBySuffix(String suffix, Iterable<Object> values) {
        String key = RedisUtil.key(canalConfig.getSchema(), tableName(), suffix);
        Set<Object> hashKeys = new HashSet<>();
        for (Object value : values) {
            hashKeys.add(value.toString());
        }
        List<Object> objects = multiGet(key, hashKeys);
        if (CollectionUtils.isEmpty(objects)) {
            return Collections.emptyList();
        }
        return objects.stream()
                .filter(Objects::nonNull)
                .map(o -> JSON.parseObject(o.toString(), tClass))
                .collect(Collectors.toList());
    }

    @Override
    public T getOne(String suffix, Object value) {
        Object o = redisTemplate.opsForHash().get(RedisUtil.key(canalConfig.getSchema(),
                tableName(), suffix), value.toString());
        if (o == null) {
            return null;
        }
        return JSON.parseObject(o.toString(), tClass);
    }

    private List<Object> multiGet(String key, Collection<Object> hashKeys) {
        return redisTemplate.opsForHash().multiGet(key, hashKeys);
    }

    private String tableName() {
        if (!StringUtils.isBlank(redisMetadata.getKey())) {
            return redisMetadata.getKey();
        }
        return ReflectionUtils.getTableName(tClass);
    }
}
