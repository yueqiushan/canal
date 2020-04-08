package com.fanxuankai.canal.flow;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.CanalEntityMetadataCache;
import com.fanxuankai.canal.metadata.TableMetadata;
import com.fanxuankai.canal.mq.*;
import com.fanxuankai.canal.redis.DeleteConsumer;
import com.fanxuankai.canal.redis.EraseConsumer;
import com.fanxuankai.canal.redis.InsertOrUpdateConsumer;
import com.fanxuankai.canal.util.App;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Otter 工厂
 *
 * @author fanxuankai
 */
@SuppressWarnings("rawtypes")
public class OtterFactory {

    private static final String REDIS = "Redis";
    private static final String XXL_MQ = "XXL-MQ";
    private static final String RABBIT_MQ = "RabbitMQ";

    /**
     * Redis Otter
     *
     * @return 没有订阅数据库表格返回 empty
     */
    public static Optional<Otter> getRedisOtter() {
        return makeConnectConfig(CanalEntityMetadataCache.getAllRedisMqMetadata(),
                App.getContext().getBean(CanalConfig.class).getRedisInstance(), REDIS)
                .map(connectConfig -> {
                    Map<CanalEntry.EventType, MessageConsumer> consumerMap = new HashMap<>(4);
                    MessageConsumer insertOrUpdateConsumer = new InsertOrUpdateConsumer();
                    consumerMap.put(CanalEntry.EventType.INSERT, insertOrUpdateConsumer);
                    consumerMap.put(CanalEntry.EventType.UPDATE, insertOrUpdateConsumer);
                    consumerMap.put(CanalEntry.EventType.DELETE, new DeleteConsumer());
                    consumerMap.put(CanalEntry.EventType.ERASE, new EraseConsumer());
                    HandleSubscriber.Config subscriberConfig = HandleSubscriber.Config.builder()
                            .handler(new MessageHandler(MessageHandler.Config.builder()
                                    .name(REDIS)
                                    .consumerMap(consumerMap)
                                    .build()))
                            .name(REDIS)
                            .build();
                    return OtterFlow.withFlow(connectConfig, subscriberConfig);
                });
    }

    /**
     * RabbitMQ Otter
     *
     * @return 没有订阅数据库表格返回 empty
     */
    public static Optional<Otter> getRabbitMqOtter() {
        CanalConfig canalConfig = App.getContext().getBean(CanalConfig.class);
        return makeConnectConfig(CanalEntityMetadataCache.getAllMqMetadata(),
                canalConfig.getMqInstance(), RABBIT_MQ)
                .map(connectConfig -> {
                    Map<CanalEntry.EventType, MessageConsumer> consumerMap = new HashMap<>(3);
                    consumerMap.put(CanalEntry.EventType.INSERT, new RabbitMqInsertConsumer());
                    consumerMap.put(CanalEntry.EventType.UPDATE, new RabbitMqUpdateConsumer());
                    consumerMap.put(CanalEntry.EventType.DELETE, new RabbitMqDeleteConsumer());
                    HandleSubscriber.Config subscriberConfig = HandleSubscriber.Config.builder()
                            .handler(new MessageHandler(MessageHandler.Config.builder()
                                    .name(RABBIT_MQ)
                                    .consumerMap(consumerMap)
                                    .build()))
                            .name(RABBIT_MQ)
                            .skip(Objects.equals(canalConfig.getSkipMq(), Boolean.TRUE))
                            .build();
                    return OtterFlow.withFlow(connectConfig, subscriberConfig);
                });
    }

    /**
     * XXL-MQ Otter
     *
     * @return 没有订阅数据库表格返回 empty
     */
    public static Optional<Otter> getXxlMqOtter() {
        CanalConfig canalConfig = App.getContext().getBean(CanalConfig.class);
        return makeConnectConfig(CanalEntityMetadataCache.getAllMqMetadata(),
                canalConfig.getMqInstance(), XXL_MQ)
                .map(connectConfig -> {
                    Map<CanalEntry.EventType, MessageConsumer> consumerMap = new HashMap<>(3);
                    consumerMap.put(CanalEntry.EventType.INSERT, new XxlMqInsertConsumer());
                    consumerMap.put(CanalEntry.EventType.UPDATE, new XxlMqUpdateConsumer());
                    consumerMap.put(CanalEntry.EventType.DELETE, new XxlMqDeleteConsumer());
                    HandleSubscriber.Config subscriberConfig = HandleSubscriber.Config.builder()
                            .handler(new MessageHandler(MessageHandler.Config.builder()
                                    .name(XXL_MQ)
                                    .consumerMap(consumerMap)
                                    .build()))
                            .name(XXL_MQ)
                            .skip(Objects.equals(canalConfig.getSkipMq(), Boolean.TRUE))
                            .build();
                    return OtterFlow.withFlow(connectConfig, subscriberConfig);
                });
    }

    /**
     * 创建 Otter 链接所需配置文件
     *
     * @param canalEntityMetadataList 所有 canal entity
     * @param instance                canal 实例
     * @param name                    订阅者
     * @return Optional<ConnectConfig>
     */
    private static Optional<ConnectConfig> makeConnectConfig(List<CanalEntityMetadata> canalEntityMetadataList,
                                                             String instance, String name) {
        List<TableMetadata> tableMetadataList = canalEntityMetadataList.stream()
                .map(CanalEntityMetadata::getTableMetadata)
                .distinct()
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(tableMetadataList)) {
            return Optional.empty();
        }
        String filter = filterString(tableMetadataList);
        ConnectConfig connectConfig = ConnectConfig.builder()
                .filter(filter)
                .instance(instance)
                .subscriberName(name)
                .build();
        return Optional.of(connectConfig);
    }

    public static String filterString(List<TableMetadata> metadataList) {
        return metadataList.stream()
                .distinct()
                .map(TableMetadata::toFilter)
                .collect(Collectors.joining(","));
    }
}
