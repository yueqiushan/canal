package com.fanxuankai.canal.annotation;

import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.MqMetadata;
import com.fanxuankai.canal.metadata.RedisMetadata;
import com.fanxuankai.canal.metadata.TableMetadata;
import com.fanxuankai.canal.util.ReflectionUtils;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class CanalEntityMetadataCache {

    private static final List<CanalEntityMetadata> ALL_METADATA = Lists.newArrayList();
    private static final List<CanalEntityMetadata> ALL_MQ_METADATA = Lists.newArrayList();
    private static final List<CanalEntityMetadata> ALL_REDIS_METADATA = Lists.newArrayList();
    private static final Map<Class<?>, CanalEntityMetadata> DATA_BY_CLASS = Maps.newHashMap();
    private static final Map<TableMetadata, CanalEntityMetadata> DATA_BY_TABLE = Maps.newHashMap();

    public static List<CanalEntityMetadata> getAllMetadata() {
        return ALL_METADATA;
    }

    public static List<CanalEntityMetadata> getAllMqMetadata() {
        return ALL_MQ_METADATA;
    }

    public static List<CanalEntityMetadata> getAllRedisMqMetadata() {
        return ALL_REDIS_METADATA;
    }

    public static CanalEntityMetadata getMetadata(Class<?> clazz) {
        return DATA_BY_CLASS.get(clazz);
    }

    public static CanalEntityMetadata getMetadata(TableMetadata tableMetadata) {
        return DATA_BY_TABLE.get(tableMetadata);
    }

    public static CanalEntityMetadata getMetadata(EntryWrapper entryWrapper) {
        TableMetadata tableMetadata = new TableMetadata(entryWrapper.getSchemaName(), entryWrapper.getTableName());
        return getMetadata(tableMetadata);
    }

    public static TableMetadata getTableMetadata(EntryWrapper entryWrapper) {
        return getMetadata(entryWrapper).getTableMetadata();
    }

    public static RedisMetadata getRedisMetadata(Class<?> clazz) {
        return getMetadata(clazz).getRedisMetadata();
    }

    public static RedisMetadata getRedisMetadata(TableMetadata tableMetadata) {
        return getMetadata(tableMetadata).getRedisMetadata();
    }

    public static RedisMetadata getRedisMetadata(EntryWrapper entryWrapper) {
        return getMetadata(entryWrapper).getRedisMetadata();
    }

    public static MqMetadata getMqMetadata(Class<?> clazz) {
        return getMetadata(clazz).getMqMetadata();
    }

    public static MqMetadata getMqMetadata(TableMetadata tableMetadata) {
        return getMetadata(tableMetadata).getMqMetadata();
    }

    public static MqMetadata getMqMetadata(EntryWrapper entryWrapper) {
        return getMetadata(entryWrapper).getMqMetadata();
    }

    public static void load() {
        List<String> scanBasePackages = EnableCanalAttributes.getScanEntityBasePackages();
        for (String scanBasePackage : scanBasePackages) {
            Map<Class<?>, CanalEntity> classMqMap = ReflectionUtils.scanAnnotation(scanBasePackage, CanalEntity.class);
            if (CollectionUtils.isEmpty(classMqMap)) {
                return;
            }
            for (Map.Entry<Class<?>, CanalEntity> entry : classMqMap.entrySet()) {
                Class<?> aClass = entry.getKey();
                CanalEntity canalEntity = entry.getValue();
                CanalEntityMetadata metadata = new CanalEntityMetadata(canalEntity, aClass);
                ALL_METADATA.add(metadata);
                if (metadata.getRedisMetadata().isEnable()) {
                    ALL_REDIS_METADATA.add(metadata);
                }
                if (metadata.getMqMetadata().isEnable()) {
                    ALL_MQ_METADATA.add(metadata);
                }
                DATA_BY_CLASS.put(aClass, metadata);
            }
        }
        DATA_BY_TABLE.putAll(ALL_METADATA.stream().collect(Collectors.toMap(CanalEntityMetadata::getTableMetadata,
                o -> o)));
    }
}
