package com.fanxuankai.canal.redis;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.flow.*;
import com.fanxuankai.canal.metadata.CanalEntityMetadataCache;
import com.fanxuankai.canal.util.App;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis Otter 并行流客户端
 *
 * @author fanxuankai
 */
public class RedisFlowOtter extends FlowOtter {

    private static final String NAME = "Redis";

    public RedisFlowOtter() {
        super(new ConnectConfig(CanalEntityMetadataCache.getAllRedisMqMetadata(),
                App.getContext().getBean(CanalConfig.class).getRedisInstance(), NAME), config());
    }

    @SuppressWarnings("rawtypes")
    private static HandleSubscriber.Config config() {
        Map<CanalEntry.EventType, MessageConsumer> consumerMap = new HashMap<>(4);
        MessageConsumer insertOrUpdateConsumer = new InsertOrUpdateConsumer();
        consumerMap.put(CanalEntry.EventType.INSERT, insertOrUpdateConsumer);
        consumerMap.put(CanalEntry.EventType.UPDATE, insertOrUpdateConsumer);
        consumerMap.put(CanalEntry.EventType.DELETE, new DeleteConsumer());
        consumerMap.put(CanalEntry.EventType.ERASE, new EraseConsumer());
        return HandleSubscriber.Config.builder()
                .handler(new MessageHandler(MessageHandler.Config.builder().name(NAME).consumerMap(consumerMap).build()))
                .name(NAME)
                .build();
    }

}
