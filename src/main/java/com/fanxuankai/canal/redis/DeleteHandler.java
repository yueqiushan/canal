package com.fanxuankai.canal.redis;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.annotation.CombineKey;
import com.fanxuankai.canal.constants.RedisConstants;
import com.fanxuankai.canal.metadata.RedisMetadata;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import com.google.common.collect.Maps;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class DeleteHandler extends AbstractRedisHandler {

    public DeleteHandler(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Override
    public void handle(EntryWrapper entryWrapper) {
        RedisMetadata redisMetadata = CanalEntityMetadataCache.getRedisMetadata(entryWrapper);
        List<String> keys = redisMetadata.getKeys();
        List<CombineKey> combineKeys = redisMetadata.getCombineKeys();
        String key = keyOf(entryWrapper);
        HashOperations<String, Object, Object> ops = redisTemplate.opsForHash();
        Map<String, List<String>> hashKeyMap = Maps.newHashMap();
        filterEntryRowData(entryWrapper, true);
        entryWrapper.getAllRowDataList()
                .forEach(rowData -> {
                    rowData.getBeforeColumnsList()
                            .stream()
                            .filter(column -> {
                                if (CollectionUtils.isEmpty(keys)) {
                                    return true;
                                }
                                return keys.contains(column.getName());
                            })
                            .forEach(o -> {
                                if (o.getIsKey()) {
                                    hashKeyMap.computeIfAbsent(key, s -> new ArrayList<>()).add(o.getValue());
                                } else if (keys.contains(o.getName())) {
                                    hashKeyMap.computeIfAbsent(keyOf(entryWrapper, o.getName()),
                                            s -> new ArrayList<>()).add(o.getValue());
                                }
                            });
                    if (!CollectionUtils.isEmpty(combineKeys)) {
                        Map<String, String> columnMap = rowData.getBeforeColumnsList().stream()
                                .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
                        for (CombineKey combineKey : combineKeys) {
                            List<String> columnList = Arrays.asList(combineKey.values());
                            String suffix = String.join(RedisConstants.SEPARATOR, columnList);
                            String name =
                                    columnList.stream().map(columnMap::get).collect(Collectors.joining(RedisConstants.SEPARATOR));
                            hashKeyMap.computeIfAbsent(keyOf(entryWrapper, suffix), s -> new ArrayList<>()).add(name);
                        }
                    }
                });
        hashKeyMap.forEach((s, strings) -> {
            Object[] objects = strings.toArray();
            ops.delete(s, objects);
        });
    }
}
