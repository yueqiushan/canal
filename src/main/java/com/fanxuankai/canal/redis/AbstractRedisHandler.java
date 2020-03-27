package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.flow.AbstractHandler;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.RedisMetadata;
import com.fanxuankai.canal.metadata.TableMetadata;
import com.fanxuankai.canal.util.RedisUtil;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Redis 抽象处理类
 *
 * @author fanxuankai
 */
public abstract class AbstractRedisHandler extends AbstractHandler {

    protected RedisTemplate<String, Object> redisTemplate;

    public AbstractRedisHandler(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    protected static void filterEntryRowData(EntryWrapper entryWrapper, boolean filterBeforeColumn) {
        filterEntryRowData(entryWrapper, metadata -> metadata.getRedisMetadata().getAviatorExpression(),
                filterBeforeColumn);
    }

    protected static String keyOf(EntryWrapper entryWrapper) {
        TableMetadata tableMetadata = CanalEntityMetadataCache.getTableMetadata(entryWrapper);
        return RedisUtil.key(tableMetadata.getSchema(), tableMetadata.getName());
    }

    protected static String keyOf(EntryWrapper entryWrapper, String suffix) {
        CanalEntityMetadata canalEntityMetadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        RedisMetadata redisMetadata = canalEntityMetadata.getRedisMetadata();
        if (StringUtils.isNotBlank(redisMetadata.getKey())) {
            return RedisUtil.customKey(redisMetadata.getKey(), suffix);
        }
        TableMetadata tableMetadata = canalEntityMetadata.getTableMetadata();
        return RedisUtil.key(tableMetadata.getSchema(), tableMetadata.getName(), suffix);
    }

    @Override
    public boolean canHandle(EntryWrapper entryWrapper) {
        CanalEntityMetadata metadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        return metadata != null && metadata.getRedisMetadata().isEnable();
    }
}
