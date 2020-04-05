package com.fanxuankai.canal.metadata;

import com.fanxuankai.canal.annotation.CanalEntity;
import lombok.Getter;

import java.util.Objects;

/**
 * CanalEntity 注解元数据
 *
 * @author fanxuankai
 */
@Getter
public class CanalEntityMetadata {
    private Class<?> domainType;
    private TableMetadata tableMetadata;
    private RedisMetadata redisMetadata;
    private MqMetadata mqMetadata;

    public CanalEntityMetadata(CanalEntity canalEntity, Class<?> domainType) {
        this.domainType = domainType;
        this.tableMetadata = new TableMetadata(canalEntity.table(), domainType);
        this.redisMetadata = new RedisMetadata(canalEntity.redis());
        this.mqMetadata = new MqMetadata(canalEntity.mq());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CanalEntityMetadata that = (CanalEntityMetadata) o;
        return Objects.equals(tableMetadata, that.tableMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableMetadata);
    }
}
