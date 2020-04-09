package com.fanxuankai.canal.mq;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.flow.*;
import com.fanxuankai.canal.metadata.CanalEntityMetadataCache;
import com.fanxuankai.canal.util.App;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * RabbitMQ  Otter 并行流客户端
 *
 * @author fanxuankai
 */
public class RabbitMqFlowOtter extends FlowOtter {

    private static final String NAME = "RabbitMQ";
    private static final CanalConfig CANAL_CONFIG = App.getContext().getBean(CanalConfig.class);

    public RabbitMqFlowOtter() {
        super(new ConnectConfig(CanalEntityMetadataCache.getAllMqMetadata(), CANAL_CONFIG.getMqInstance(), NAME),
                config());
    }

    @SuppressWarnings("rawtypes")
    private static HandleSubscriber.Config config() {
        Map<CanalEntry.EventType, MessageConsumer> consumerMap = new HashMap<>(3);
        consumerMap.put(CanalEntry.EventType.INSERT, new RabbitMqInsertConsumer());
        consumerMap.put(CanalEntry.EventType.UPDATE, new RabbitMqUpdateConsumer());
        consumerMap.put(CanalEntry.EventType.DELETE, new RabbitMqDeleteConsumer());
        return HandleSubscriber.Config.builder()
                .handler(new MessageHandler(MessageHandler.Config.builder().name(NAME).consumerMap(consumerMap).build()))
                .name(NAME)
                .skip(Objects.equals(CANAL_CONFIG.getSkipMq(), Boolean.TRUE))
                .build();
    }

}
