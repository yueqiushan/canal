package com.fanxuankai.canal.metadata;

import com.fanxuankai.canal.annotation.CanalEntity;
import com.fanxuankai.canal.constants.CommonConstants;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.reflections.Reflections;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CanalEntity 注解元数据缓存
 *
 * @author fanxuankai
 */
public class CanalEntityMetadataCache {

    private static final List<CanalEntityMetadata> ALL_METADATA = Lists.newArrayList();
    private static final List<CanalEntityMetadata> ALL_MQ_METADATA = Lists.newArrayList();
    private static final List<CanalEntityMetadata> ALL_REDIS_METADATA = Lists.newArrayList();
    private static final Map<Class<?>, CanalEntityMetadata> DATA_BY_CLASS = Maps.newHashMap();
    private static final Map<String, CanalEntityMetadata> DATA_BY_TABLE = Maps.newHashMap();

    public static List<CanalEntityMetadata> getAllMqMetadata() {
        return ALL_MQ_METADATA;
    }

    public static List<CanalEntityMetadata> getAllRedisMqMetadata() {
        return ALL_REDIS_METADATA;
    }

    public static CanalEntityMetadata getMetadata(Class<?> clazz) {
        return DATA_BY_CLASS.get(clazz);
    }

    public static CanalEntityMetadata getMetadata(EntryWrapper entryWrapper) {
        return DATA_BY_TABLE.get(entryWrapper.getSchemaName() + CommonConstants.SEPARATOR + entryWrapper.getTableName());
    }

    public static RedisMetadata getRedisMetadata(EntryWrapper entryWrapper) {
        return getMetadata(entryWrapper).getRedisMetadata();
    }

    public static void from(Reflections r) {
        Set<Class<?>> canalEntityClasses = r.getTypesAnnotatedWith(CanalEntity.class);
        Map<? extends Class<?>, CanalEntity> map = canalEntityClasses.stream()
                .collect(Collectors.toMap(o -> o, o -> o.getAnnotation(CanalEntity.class)));
        if (CollectionUtils.isEmpty(map)) {
            return;
        }
        for (Map.Entry<? extends Class<?>, CanalEntity> entry : map.entrySet()) {
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
        DATA_BY_TABLE.putAll(ALL_METADATA.stream()
                .collect(Collectors.toMap(o -> {
                    TableMetadata tableMetadata = o.getTableMetadata();
                    return tableMetadata.getSchema() + CommonConstants.SEPARATOR + tableMetadata.getName();
                }, o -> o)));
    }
}
