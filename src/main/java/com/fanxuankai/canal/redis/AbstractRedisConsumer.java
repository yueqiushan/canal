package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.flow.MessageConsumer;
import com.fanxuankai.canal.metadata.*;
import com.fanxuankai.canal.util.App;
import com.fanxuankai.canal.util.RedisUtils;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractRedisConsumer<R> implements MessageConsumer<R> {

    private static Map<TableMetadata, String> keyCache = new ConcurrentHashMap<>();
    private static Map<SuffixKey, String> suffixKeyCache = new ConcurrentHashMap<>();
    protected RedisTemplate<String, Object> redisTemplate = App.getRedisTemplate();

    @Override
    public boolean canProcess(EntryWrapper entryWrapper) {
        CanalEntityMetadata metadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        return metadata != null && metadata.getRedisMetadata().isEnable();
    }

    @Override
    public FilterMetadata filter(CanalEntityMetadata metadata) {
        return metadata.getRedisMetadata().getFilterMetadata();
    }

    protected String keyOf(EntryWrapper entryWrapper) {
        CanalEntityMetadata canalEntityMetadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        RedisMetadata redisMetadata = canalEntityMetadata.getRedisMetadata();
        TableMetadata tableMetadata = canalEntityMetadata.getTableMetadata();
        return keyCache(tableMetadata, redisMetadata);
    }

    protected String keyOf(EntryWrapper entryWrapper, String suffix) {
        CanalEntityMetadata canalEntityMetadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        TableMetadata tableMetadata = canalEntityMetadata.getTableMetadata();
        RedisMetadata redisMetadata = canalEntityMetadata.getRedisMetadata();
        return keyCache(tableMetadata, redisMetadata, suffix);
    }

    private String key(TableMetadata tableMetadata, RedisMetadata redisMetadata) {
        if (StringUtils.isNotBlank(redisMetadata.getKey())) {
            return redisMetadata.getKey();
        }
        return RedisUtils.key(tableMetadata.getSchema(), tableMetadata.getName());
    }

    private String keyCache(TableMetadata tableMetadata, RedisMetadata redisMetadata) {
        String key = keyCache.get(tableMetadata);
        if (key == null) {
            key = key(tableMetadata, redisMetadata);
            keyCache.put(tableMetadata, key);
        }
        return key;
    }

    private String keyCache(TableMetadata tableMetadata, RedisMetadata redisMetadata, String suffix) {
        SuffixKey suffixKey = new SuffixKey(tableMetadata, suffix);
        String key = suffixKeyCache.get(suffixKey);
        if (key == null) {
            key = RedisUtils.customKey(keyCache(tableMetadata, redisMetadata), suffix);
            suffixKeyCache.put(suffixKey, key);
        }
        return key;
    }

    private static class SuffixKey {
        private TableMetadata tableMetadata;
        private String suffix;

        public SuffixKey(TableMetadata tableMetadata, String suffix) {
            this.tableMetadata = tableMetadata;
            this.suffix = suffix;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SuffixKey suffixKey = (SuffixKey) o;
            return tableMetadata.equals(suffixKey.tableMetadata) &&
                    suffix.equals(suffixKey.suffix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableMetadata, suffix);
        }
    }

}
