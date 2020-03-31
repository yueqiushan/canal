package com.fanxuankai.canal.flow;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.TableMetadata;
import com.fanxuankai.canal.mq.*;
import com.fanxuankai.canal.redis.DeleteHandler;
import com.fanxuankai.canal.redis.EraseHandler;
import com.fanxuankai.canal.redis.InsertOrUpdateHandler;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class OtterFactory {

    private static final String REDIS = "Redis";
    private static final String XXL_MQ = "XXL-MQ";
    private static final String RABBIT_MQ = "RabbitMQ";

    private static Optional<ConnectConfig> makeConnectConfig(CanalConfig canalConfig,
                                                             Predicate<? super CanalEntityMetadata> predicate,
                                                             String instance, String name) {
        List<TableMetadata> tableMetadataList = CanalEntityMetadataCache.getAllMetadata()
                .stream()
                .filter(predicate)
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

    public static Optional<Otter> getRedisOtter(CanalConfig canalConfig, RedisTemplate<String, Object> redisTemplate) {
        Predicate<CanalEntityMetadata> predicate =
                canalEntityMetadata -> canalEntityMetadata.getRedisMetadata().isEnable();
        String name = "Redis";
        return makeConnectConfig(canalConfig, predicate,
                canalConfig.getRedisInstance(), name)
                .map(connectConfig -> {
                    Map<CanalEntry.EventType, Handler> handlerMap = new HashMap<>(4);
                    InsertOrUpdateHandler insertOrUpdateHandler = new InsertOrUpdateHandler(redisTemplate);
                    handlerMap.put(CanalEntry.EventType.INSERT, insertOrUpdateHandler);
                    handlerMap.put(CanalEntry.EventType.UPDATE, insertOrUpdateHandler);
                    handlerMap.put(CanalEntry.EventType.DELETE, new DeleteHandler(redisTemplate));
                    handlerMap.put(CanalEntry.EventType.ERASE, new EraseHandler(redisTemplate));
                    HandleSubscriber.Config subscriberConfig = HandleSubscriber.Config.builder()
                            .handlerMap(handlerMap)
                            .redisTemplate(redisTemplate)
                            .logfileOffsetPrefix(REDIS)
                            .canalConfig(canalConfig)
                            .subscriberName(name)
                            .build();
                    return OtterFlow.withFlow(connectConfig, subscriberConfig);
                });
    }

    public static Optional<Otter> getRabbitMqOtter(CanalConfig canalConfig, AmqpTemplate amqpTemplate,
                                                   RedisTemplate<String, Object> redisTemplate) {
        Predicate<CanalEntityMetadata> predicate =
                canalEntityMetadata -> canalEntityMetadata.getMqMetadata().isEnable();
        String name = "RabbitMq";
        return makeConnectConfig(canalConfig, predicate,
                canalConfig.getMqInstance(), name)
                .map(connectConfig -> {
                    Map<CanalEntry.EventType, Handler> handlerMap = new HashMap<>(3);
                    handlerMap.put(CanalEntry.EventType.INSERT, new RabbitMqInsertHandler(amqpTemplate));
                    handlerMap.put(CanalEntry.EventType.UPDATE, new RabbitMqUpdateHandler(amqpTemplate));
                    handlerMap.put(CanalEntry.EventType.DELETE, new RabbitMqDeleteHandler(amqpTemplate));
                    HandleSubscriber.Config subscriberConfig = HandleSubscriber.Config.builder()
                            .handlerMap(handlerMap)
                            .redisTemplate(redisTemplate)
                            .logfileOffsetPrefix(RABBIT_MQ)
                            .canalConfig(canalConfig)
                            .subscriberName(name)
                            .skip(Objects.equals(canalConfig.getSkipMq(), Boolean.TRUE))
                            .build();
                    return OtterFlow.withFlow(connectConfig, subscriberConfig);
                });
    }

    public static Optional<Otter> getXxlMqOtter(CanalConfig canalConfig, RedisTemplate<String, Object> redisTemplate) {
        Predicate<CanalEntityMetadata> predicate =
                canalEntityMetadata -> canalEntityMetadata.getMqMetadata().isEnable();
        String name = "XxlMq";
        return makeConnectConfig(canalConfig, predicate,
                canalConfig.getMqInstance(), name)
                .map(connectConfig -> {
                    Map<CanalEntry.EventType, Handler> handlerMap = new HashMap<>(3);
                    handlerMap.put(CanalEntry.EventType.INSERT, new XxlMqInsertHandler());
                    handlerMap.put(CanalEntry.EventType.UPDATE, new XxlMqUpdateHandler());
                    handlerMap.put(CanalEntry.EventType.DELETE, new XxlMqDeleteHandler());
                    HandleSubscriber.Config subscriberConfig = HandleSubscriber.Config.builder()
                            .handlerMap(handlerMap)
                            .redisTemplate(redisTemplate)
                            .logfileOffsetPrefix(XXL_MQ)
                            .canalConfig(canalConfig)
                            .subscriberName(name)
                            .skip(Objects.equals(canalConfig.getSkipMq(), Boolean.TRUE))
                            .build();
                    return OtterFlow.withFlow(connectConfig, subscriberConfig);
                });
    }
}
