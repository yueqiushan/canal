package com.fanxuankai.canal.flow;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.TableMetadata;
import com.fanxuankai.canal.mq.*;
import com.fanxuankai.canal.redis.DeleteConsumer;
import com.fanxuankai.canal.redis.EraseConsumer;
import com.fanxuankai.canal.redis.InsertOrUpdateConsumer;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.data.redis.core.RedisTemplate;
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
     * @param canalConfig   配置文件
     * @param redisTemplate RedisTemplate
     * @return 没有订阅数据库表格返回 empty
     */
    public static Optional<Otter> getRedisOtter(CanalConfig canalConfig, RedisTemplate<String, Object> redisTemplate) {
        String name = "Redis";
        return makeConnectConfig(canalConfig, CanalEntityMetadataCache.getAllRedisMqMetadata(),
                canalConfig.getRedisInstance(), name)
                .map(connectConfig -> {
                    Map<CanalEntry.EventType, MessageConsumer> handlerMap = new HashMap<>(4);
                    MessageConsumer insertOrUpdateConsumer = new InsertOrUpdateConsumer(redisTemplate);
                    handlerMap.put(CanalEntry.EventType.INSERT, insertOrUpdateConsumer);
                    handlerMap.put(CanalEntry.EventType.UPDATE, insertOrUpdateConsumer);
                    handlerMap.put(CanalEntry.EventType.DELETE, new DeleteConsumer(redisTemplate));
                    handlerMap.put(CanalEntry.EventType.ERASE, new EraseConsumer(redisTemplate));
                    HandleSubscriber.Config subscriberConfig = HandleSubscriber.Config.builder()
                            .handler(new MessageHandler(MessageHandler.Config.builder()
                                    .canalConfig(canalConfig)
                                    .logfileOffsetPrefix(REDIS)
                                    .redisTemplate(redisTemplate)
                                    .name(name)
                                    .build(), handlerMap))
                            .name(name)
                            .build();
                    return OtterFlow.withFlow(connectConfig, subscriberConfig);
                });
    }

    /**
     * RabbitMQ Otter
     *
     * @param canalConfig   配置文件
     * @param amqpTemplate  AmqpTemplate
     * @param redisTemplate RedisTemplate
     * @return 没有订阅数据库表格返回 empty
     */
    public static Optional<Otter> getRabbitMqOtter(CanalConfig canalConfig, AmqpTemplate amqpTemplate,
                                                   RedisTemplate<String, Object> redisTemplate) {
        String name = "RabbitMq";
        return makeConnectConfig(canalConfig, CanalEntityMetadataCache.getAllMqMetadata(),
                canalConfig.getMqInstance(), name)
                .map(connectConfig -> {
                    Map<CanalEntry.EventType, MessageConsumer> handlerMap = new HashMap<>(3);
                    handlerMap.put(CanalEntry.EventType.INSERT, new RabbitMqInsertConsumer(amqpTemplate));
                    handlerMap.put(CanalEntry.EventType.UPDATE, new RabbitMqUpdateConsumer(amqpTemplate));
                    handlerMap.put(CanalEntry.EventType.DELETE, new RabbitMqDeleteConsumer(amqpTemplate));
                    HandleSubscriber.Config subscriberConfig = HandleSubscriber.Config.builder()
                            .handler(new MessageHandler(MessageHandler.Config.builder()
                                    .canalConfig(canalConfig)
                                    .logfileOffsetPrefix(RABBIT_MQ)
                                    .redisTemplate(redisTemplate)
                                    .name(name)
                                    .build(), handlerMap))
                            .name(name)
                            .skip(Objects.equals(canalConfig.getSkipMq(), Boolean.TRUE))
                            .build();
                    return OtterFlow.withFlow(connectConfig, subscriberConfig);
                });
    }

    /**
     * XXL-MQ Otter
     *
     * @param canalConfig   配置文件
     * @param redisTemplate RedisTemplate
     * @return 没有订阅数据库表格返回 empty
     */
    public static Optional<Otter> getXxlMqOtter(CanalConfig canalConfig, RedisTemplate<String, Object> redisTemplate) {
        String name = "XxlMq";
        return makeConnectConfig(canalConfig, CanalEntityMetadataCache.getAllMqMetadata(),
                canalConfig.getMqInstance(), name)
                .map(connectConfig -> {
                    Map<CanalEntry.EventType, MessageConsumer> handlerMap = new HashMap<>(3);
                    handlerMap.put(CanalEntry.EventType.INSERT, new XxlMqInsertConsumer());
                    handlerMap.put(CanalEntry.EventType.UPDATE, new XxlMqUpdateConsumer());
                    handlerMap.put(CanalEntry.EventType.DELETE, new XxlMqDeleteConsumer());
                    HandleSubscriber.Config subscriberConfig = HandleSubscriber.Config.builder()
                            .handler(new MessageHandler(MessageHandler.Config.builder()
                                    .canalConfig(canalConfig)
                                    .logfileOffsetPrefix(XXL_MQ)
                                    .redisTemplate(redisTemplate)
                                    .name(name)
                                    .build(), handlerMap))
                            .name(name)
                            .skip(Objects.equals(canalConfig.getSkipMq(), Boolean.TRUE))
                            .build();
                    return OtterFlow.withFlow(connectConfig, subscriberConfig);
                });
    }

    /**
     * 创建 Otter 连接所需配置文件
     *
     * @param canalConfig             配置文件
     * @param canalEntityMetadataList 所有 canal entity
     * @param instance                canal 实例
     * @param name                    订阅者
     * @return Optional<ConnectConfig>
     */
    private static Optional<ConnectConfig> makeConnectConfig(CanalConfig canalConfig,
                                                             List<CanalEntityMetadata> canalEntityMetadataList,
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
                .canalConfig(canalConfig)
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
