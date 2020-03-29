package com.fanxuankai.canal.metadata;

import com.fanxuankai.canal.annotation.CombineKey;
import com.fanxuankai.canal.redis.Redis;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

/**
 * Redis 注解元数据
 *
 * @author fanxuankai
 */
@Getter
public class RedisMetadata {
    private boolean enable;
    private String key;
    private boolean idAsField;
    private List<String> keys;
    private List<CombineKey> combineKeys;
    private FilterMetadata filterMetadata;

    public RedisMetadata(Redis redis) {
        this.enable = redis.enable();
        this.key = redis.key();
        this.idAsField = redis.idAsField();
        this.keys = Arrays.asList(redis.uniqueKeys());
        this.combineKeys = Arrays.asList(redis.combineKeys());
        this.filterMetadata = new FilterMetadata(redis.filter());
    }

}
