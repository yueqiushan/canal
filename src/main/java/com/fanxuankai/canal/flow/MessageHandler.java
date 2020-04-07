package com.fanxuankai.canal.flow;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.aviator.Aviators;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.enums.RedisKeyPrefix;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.CanalEntityMetadataCache;
import com.fanxuankai.canal.metadata.EnableCanalAttributes;
import com.fanxuankai.canal.metadata.FilterMetadata;
import com.fanxuankai.canal.util.App;
import com.fanxuankai.canal.util.CommonUtils;
import com.fanxuankai.canal.util.RedisUtils;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import com.fanxuankai.canal.wrapper.MessageWrapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.fanxuankai.canal.constants.CommonConstants.SEPARATOR;
import static com.fanxuankai.canal.constants.RedisConstants.LOGFILE_OFFSET;

/**
 * Message 处理器
 *
 * @author fanxuankai
 */
@Slf4j
@SuppressWarnings("rawtypes unchecked")
public class MessageHandler implements Handler<MessageWrapper> {

    private Config config;
    private String logFileOffsetTag;
    private RedisTemplate<String, Object> redisTemplate;
    private CanalConfig canalConfig;

    public MessageHandler(Config config) {
        this.config = config;
        this.logFileOffsetTag = RedisUtils.customKey(RedisKeyPrefix.SERVICE_CACHE,
                EnableCanalAttributes.getName() + SEPARATOR + config.logfileOffsetPrefix + SEPARATOR + LOGFILE_OFFSET);
        this.redisTemplate = App.getRedisTemplate();
        this.canalConfig = App.getContext().getBean(CanalConfig.class);
    }

    @Override
    public void handle(MessageWrapper messageWrapper) {
        long l = System.currentTimeMillis();
        List<EntryWrapper> entryWrapperList = messageWrapper.getEntryWrapperList();
        entryWrapperList.removeIf(EntryWrapper::isDdl);
        int rowChangeDataCount = 0;
        if (!CollectionUtils.isEmpty(entryWrapperList)) {
            try {
                if (messageWrapper.getAllRawRowDataCount() >= canalConfig.getPerformanceThreshold()) {
                    rowChangeDataCount = doHandlePerformance(entryWrapperList, messageWrapper.getBatchId());
                } else {
                    rowChangeDataCount = doHandle(entryWrapperList, messageWrapper.getBatchId());
                }
            } catch (Exception e) {
                throw new HandleException(e);
            }
        }
        log.info("{} Consume batchId: {}, rowDataCount: {}({}), time: {}ms", config.name, messageWrapper.getBatchId(),
                rowChangeDataCount, messageWrapper.getAllRawRowDataCount(), System.currentTimeMillis() - l);
    }

    private int doHandle(List<EntryWrapper> entryWrapperList, long batchId) {
        int rowChangeDataCount = 0;
        for (EntryWrapper entryWrapper : entryWrapperList) {
            if (existsLogfileOffset(entryWrapper, batchId)) {
                continue;
            }
            MessageConsumer consumer = config.consumerMap.get(entryWrapper.getEventType());
            if (consumer == null) {
                throw new HandleException("无消费者");
            }
            if (consumer.canProcess(entryWrapper)) {
                Object process = process(consumer, entryWrapper);
                if (ObjectUtils.isEmpty(process)) {
                    continue;
                }
                long time = consume(consumer, process, entryWrapper);
                rowChangeDataCount += entryWrapper.getAllRowDataList().size();
                log(entryWrapper, consumer, batchId, time);
            }
        }
        return rowChangeDataCount;
    }

    private Object process(MessageConsumer consumer, EntryWrapper entryWrapper) {
        CanalEntityMetadata metadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        FilterMetadata filterMetadata = consumer.filter(metadata);
        filterEntryRowData(entryWrapper, metadata, filterMetadata);
        return consumer.process(entryWrapper);
    }

    private long consume(MessageConsumer consumer, Object process, EntryWrapper entryWrapper) {
        long ll = System.currentTimeMillis();
        consumer.consume(process);
        long time = System.currentTimeMillis() - ll;
        putOffset(entryWrapper.getLogfileName(), entryWrapper.getLogfileOffset());
        return time;
    }

    private int doHandlePerformance(List<EntryWrapper> entryWrapperList, long batchId) throws Exception {
        // 异步处理
        List<Future<EntryWrapperProcess>> futureList = entryWrapperList.stream()
                .map(entryWrapper -> ForkJoinPool.commonPool().submit(() -> {
                    MessageConsumer consumer = config.consumerMap.get(entryWrapper.getEventType());
                    if (consumer == null) {
                        throw new HandleException("无消费者");
                    }
                    Object process = null;
                    if (!existsLogfileOffset(entryWrapper, batchId)
                            && consumer.canProcess(entryWrapper)) {
                        process = process(consumer, entryWrapper);
                    }
                    return new EntryWrapperProcess(entryWrapper, process, consumer);
                }))
                .collect(Collectors.toList());
        // 顺序消费
        int rowChangeDataCount = 0;
        for (Future<EntryWrapperProcess> future : futureList) {
            EntryWrapperProcess entryWrapperProcess = future.get();
            Object process = entryWrapperProcess.process;
            if (!ObjectUtils.isEmpty(process)) {
                EntryWrapper entryWrapper = entryWrapperProcess.entryWrapper;
                MessageConsumer consumer = entryWrapperProcess.consumer;
                long time = consume(consumer, process, entryWrapper);
                rowChangeDataCount += entryWrapper.getAllRowDataList().size();
                log(entryWrapper, consumer, batchId, time);
            }
        }
        return rowChangeDataCount;
    }

    private void filterEntryRowData(EntryWrapper entryWrapper,
                                    CanalEntityMetadata metadata,
                                    FilterMetadata filterMetadata) {
        List<CanalEntry.RowData> rowDataList = entryWrapper.getAllRowDataList()
                .stream()
                .filter(rowData -> filterRowData(rowData, filterMetadata, metadata.getDomainType()))
                .collect(Collectors.toList());
        entryWrapper.setAllRowDataList(rowDataList);
    }

    private boolean filterRowData(CanalEntry.RowData rowData, FilterMetadata filterMetadata, Class<?> domainType) {
        Map<String, CanalEntry.Column> beforeColumnMap = CommonUtils.toColumnMap(rowData.getBeforeColumnsList());
        Map<String, CanalEntry.Column> afterColumnMap = CommonUtils.toColumnMap(rowData.getAfterColumnsList());
        List<String> updatedFields = filterMetadata.getUpdatedFields();
        if (!CollectionUtils.isEmpty(updatedFields)) {
            // 新增或者修改
            if (!CollectionUtils.isEmpty(afterColumnMap)) {
                boolean allMatch = afterColumnMap.entrySet()
                        .stream()
                        .filter(entry -> updatedFields.contains(entry.getKey()))
                        .allMatch(entry -> {
                            CanalEntry.Column oldColumn = beforeColumnMap.get(entry.getKey());
                            return oldColumn == null || entry.getValue().getUpdated();
                        });
                if (!allMatch) {
                    return false;
                }
            }
            // 删除默认为已全部修改
        }
        String aviatorExpression = filterMetadata.getAviatorExpression();
        if (StringUtils.isNotBlank(aviatorExpression)) {
            // 新增或者修改
            if (!CollectionUtils.isEmpty(afterColumnMap)) {
                return Aviators.exec(CommonUtils.toMap(rowData.getAfterColumnsList()), aviatorExpression, domainType);
            }
            // 删除
            if (!CollectionUtils.isEmpty(beforeColumnMap)) {
                return Aviators.exec(CommonUtils.toMap(rowData.getBeforeColumnsList()), aviatorExpression, domainType);
            }
        }
        return true;
    }

    private boolean existsLogfileOffset(EntryWrapper entryWrapper, long batchId) throws HandleException {
        String logfileName = entryWrapper.getLogfileName();
        long logfileOffset = entryWrapper.getLogfileOffset();
        if (existsLogfileOffset(logfileName, logfileOffset)) {
            ExistsLogfileOffset existsLogfileOffset = ExistsLogfileOffset.builder()
                    .name(config.logfileOffsetPrefix)
                    .batchId(batchId)
                    .schema(entryWrapper.getSchemaName())
                    .table(entryWrapper.getTableName())
                    .logfileName(logfileName)
                    .logfileOffset(logfileOffset)
                    .build();
            log.info("防重消费: {}", JSON.toJSONString(existsLogfileOffset));
            return true;
        }
        return false;
    }

    private void log(EntryWrapper entryWrapper, MessageConsumer consumer, long batchId, long time) {
        if (Objects.equals(canalConfig.getShowLog(), Boolean.TRUE)) {
            MessageHandlerLogger.asyncLog(MessageHandlerLogger.LogInfo.builder()
                    .clazz(consumer.getClass())
                    .entryWrapper(entryWrapper)
                    .batchId(batchId)
                    .time(time)
                    .build());
        }
    }

    private boolean existsLogfileOffset(String logfileName, long offset) {
        Object value = redisTemplate.opsForHash().get(logFileOffsetTag, logfileName);
        if (value == null) {
            return false;
        }
        return Long.parseLong(value.toString()) >= offset;
    }

    private void putOffset(String logfileName, long offset) {
        redisTemplate.opsForHash().put(logFileOffsetTag, logfileName, offset);
    }

    @Getter
    @Builder
    public static class Config {
        private String name;
        private String logfileOffsetPrefix;
        private Map<CanalEntry.EventType, MessageConsumer> consumerMap;
    }

    @Getter
    @Builder
    private static class ExistsLogfileOffset {
        private String name;
        private long batchId;
        private String schema;
        private String table;
        private String logfileName;
        private long logfileOffset;
    }

    @Getter
    @AllArgsConstructor
    private static class EntryWrapperProcess {
        private EntryWrapper entryWrapper;
        private Object process;
        private MessageConsumer consumer;
    }

}
