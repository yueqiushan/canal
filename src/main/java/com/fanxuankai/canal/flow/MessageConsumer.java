package com.fanxuankai.canal.flow;

import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.FilterMetadata;
import com.fanxuankai.canal.wrapper.EntryWrapper;

/**
 * Message 消费者
 *
 * @author fanxuankai
 */
public interface MessageConsumer<R> extends Consumer<EntryWrapper, R> {

    /**
     * 过滤
     *
     * @param metadata CanalEntity 注解元数据
     * @return Filter 注解元数据
     */
    FilterMetadata filter(CanalEntityMetadata metadata);

}
