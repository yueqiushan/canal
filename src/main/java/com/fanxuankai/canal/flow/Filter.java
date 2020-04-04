package com.fanxuankai.canal.flow;

import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.FilterMetadata;

/**
 * @author fanxuankai
 */
public interface Filter {
    /**
     * 过滤
     *
     * @param metadata CanalEntity 注解元数据
     * @return Filter 注解元数据
     */
    FilterMetadata filter(CanalEntityMetadata metadata);
}
