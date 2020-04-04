package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.Set;

/**
 * @author fanxuankai
 */
public class EraseConsumer extends AbstractRedisConsumer<Set<String>> {

    public EraseConsumer(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Override
    public Set<String> process(EntryWrapper entryWrapper) {
        Set<String> keys = redisTemplate.keys(keyOf(entryWrapper) + "*");
        if (CollectionUtils.isEmpty(keys)) {
            return Collections.emptySet();
        }
        return keys;
    }

    @Override
    public void consume(Set<String> strings) {
        redisTemplate.delete(strings);
    }
}
