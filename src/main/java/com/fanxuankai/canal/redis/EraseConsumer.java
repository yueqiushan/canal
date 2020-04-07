package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.Set;

/**
 * 数据库删除事件消费者
 *
 * @author fanxuankai
 */
public class EraseConsumer extends AbstractRedisConsumer<Set<String>> {

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
