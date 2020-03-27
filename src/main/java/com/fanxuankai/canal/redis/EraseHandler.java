package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.Set;

/**
 * @author fanxuankai
 */
public class EraseHandler extends AbstractRedisHandler {

    public EraseHandler(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Override
    public void handle(EntryWrapper entryWrapper) {
        String key = keyOf(entryWrapper);
        Set<String> keys = redisTemplate.keys(key + "*");
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }
        redisTemplate.delete(keys);
    }
}
