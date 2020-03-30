package com.fanxuankai.canal.flow;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.enums.RedisKeyPrefix;
import com.fanxuankai.canal.util.RedisUtils;
import com.fanxuankai.canal.util.ThreadPoolService;
import com.fanxuankai.canal.wrapper.ContextWrapper;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

import static com.fanxuankai.canal.constants.RedisConstants.LOGFILE_OFFSET;
import static com.fanxuankai.canal.constants.RedisConstants.SEPARATOR;

/**
 * @author fanxuankai
 */
@Slf4j
public class HandleSubscriber extends SubmissionPublisher<ContextWrapper> implements Flow.Processor<ContextWrapper,
        ContextWrapper> {
    private Config config;
    private String logFileOffsetTag;
    private Flow.Subscription subscription;

    public HandleSubscriber(Config config) {
        this.config = config;
        logFileOffsetTag = RedisUtils.customKey(RedisKeyPrefix.SERVICE_CACHE,
                config.logfileOffsetPrefix + SEPARATOR + LOGFILE_OFFSET);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(ContextWrapper item) {
        long batchId = item.getMessageWrapper().getBatchId();
        List<EntryWrapper> entryWrapperList = item.getMessageWrapper().getEntryWrapperList();
        entryWrapperList.removeIf(EntryWrapper::isDdl);
        long l = System.currentTimeMillis();
        int rowChangeDataCount = 0;
        try {
            if (!config.skip) {
                for (EntryWrapper entryWrapper : entryWrapperList) {
                    handle(entryWrapper, batchId);
                    rowChangeDataCount += entryWrapper.getAllRowDataList().size();
                }
            }
            item.setProcessed(true);
        } catch (HandleException e) {
            throw new RuntimeException("Handle Exception batchId: " + batchId, e);
        } finally {
            submit(item);
        }
        String handle = config.skip ? "Skip Handle" : "Handle";
        log.info("{} {} batchId: {}, rowDataCount: {}({}), time: {}ms", config.subscriberName, handle, batchId,
                rowChangeDataCount, item.getAllRawRowDataCount(), System.currentTimeMillis() - l);
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        log.error(throwable.getLocalizedMessage(), throwable);
    }

    @Override
    public void onComplete() {
        log.info("Done");
    }

    private void handle(EntryWrapper entryWrapper, long batchId) throws HandleException {
        String logfileName = entryWrapper.getLogfileName();
        long logfileOffset = entryWrapper.getLogfileOffset();
        Handler handler = config.handlerMap.get(entryWrapper.getEventType());
        if (handler == null) {
            throw new HandleException("无处理器 batchId: " + batchId);
        }
        if (existsOffset(logfileName, logfileOffset)) {
            ThreadPoolService.getInstance().execute(() -> {
                LogExistsOffset logExistsOffset = LogExistsOffset.builder()
                        .name(config.logfileOffsetPrefix)
                        .batchId(batchId)
                        .schema(entryWrapper.getSchemaName())
                        .table(entryWrapper.getTableName())
                        .logfileName(logfileName)
                        .logfileOffset(logfileOffset)
                        .build();
                log.info("防重消费: {}", JSON.toJSONString(logExistsOffset));
            });
            return;
        }
        long l = System.currentTimeMillis();
        try {
            handler.handle(entryWrapper);
        } catch (Exception e) {
            throw new HandleException(e);
        }
        if (Objects.equals(config.canalConfig.getShowLog(), Boolean.TRUE)) {
            HandlerLogger.asyncLog(HandlerLogger.LogInfo.builder()
                    .canalConfig(config.canalConfig)
                    .handler(handler)
                    .entryWrapper(entryWrapper)
                    .batchId(batchId)
                    .time(System.currentTimeMillis() - l)
                    .build());
        }
        putOffset(logfileName, logfileOffset);
    }

    private boolean existsOffset(String logfileName, long offset) {
        Object value = config.redisTemplate.opsForHash().get(logFileOffsetTag, logfileName);
        if (value == null) {
            return false;
        }
        return Long.parseLong(value.toString()) >= offset;
    }

    private void putOffset(String logfileName, long offset) {
        config.redisTemplate.opsForHash().put(logFileOffsetTag, logfileName, offset);
    }

    @Data
    @Builder
    public static class Config {
        private Map<CanalEntry.EventType, Handler> handlerMap;
        private CanalConfig canalConfig;
        private RedisTemplate<String, Object> redisTemplate;
        private String logfileOffsetPrefix;
        private String subscriberName;
        private boolean skip;
    }

    @Data
    @Builder
    private static class LogExistsOffset {
        private String name;
        private long batchId;
        private String schema;
        private String table;
        private String logfileName;
        private long logfileOffset;
    }
}
