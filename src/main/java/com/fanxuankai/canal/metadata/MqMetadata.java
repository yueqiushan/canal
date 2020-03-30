package com.fanxuankai.canal.metadata;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.mq.Mq;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

/**
 * MQ 注解元数据
 *
 * @author fanxuankai
 */
@Getter
public class MqMetadata {
    private boolean enable;
    private String name;
    private List<CanalEntry.EventType> eventTypes;
    private FilterMetadata filterMetadata;

    public MqMetadata(Mq mq) {
        this.enable = mq.enable();
        this.name = mq.name();
        this.eventTypes = Arrays.asList(mq.eventTypes());
        this.filterMetadata = new FilterMetadata(mq.filter());
    }
}
