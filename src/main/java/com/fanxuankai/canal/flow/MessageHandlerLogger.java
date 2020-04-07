package com.fanxuankai.canal.flow;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.util.App;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

/**
 * @author fanxuankai
 */
@Slf4j
public class MessageHandlerLogger {

    public static void asyncLog(LogInfo logInfo) {
        ForkJoinPool.commonPool().execute(() -> {
            EntryWrapper entryWrapper = logInfo.entryWrapper;
            log.info(JSON.toJSONString(LogRowChange.builder()
                    .handler(logInfo.clazz.getName())
                    .batchId(logInfo.batchId)
                    .file(entryWrapper.getLogfileName())
                    .offset(entryWrapper.getLogfileOffset())
                    .schema(entryWrapper.getSchemaName())
                    .table(entryWrapper.getTableName())
                    .eventType(entryWrapper.getEventType())
                    .count(entryWrapper.getAllRowDataList().size())
                    .time(logInfo.time)
                    .build()));
            CanalConfig canalConfig = App.getContext().getBean(CanalConfig.class);
            if (Objects.equals(canalConfig.getShowRowChange(), Boolean.TRUE)) {
                List<List<LogColumn>> list = entryWrapper.getAllRowDataList().stream()
                        .map(o -> logColumns(o, entryWrapper.getEventType()))
                        .collect(Collectors.toList());
                log.info(JSON.toJSONString(list,
                        Objects.equals(canalConfig.getFormatRowChangeLog(), Boolean.TRUE)));
            }
        });
    }

    private static List<LogColumn> logColumns(CanalEntry.RowData rowData, CanalEntry.EventType eventType) {
        if (eventType == DELETE || eventType == ERASE) {
            return rowData.getBeforeColumnsList().stream()
                    .map(column -> LogColumn.builder().name(column.getName()).oldValue(column.getValue()).build())
                    .collect(Collectors.toList());
        } else if (eventType == INSERT) {
            return rowData.getAfterColumnsList().stream()
                    .map(column -> LogColumn.builder().name(column.getName()).oldValue(column.getValue()).build())
                    .collect(Collectors.toList());
        } else if (eventType == UPDATE) {
            List<LogColumn> logColumns = new ArrayList<>(rowData.getAfterColumnsCount());
            for (int i = 0; i < rowData.getAfterColumnsList().size(); i++) {
                CanalEntry.Column bColumn = rowData.getBeforeColumnsList().get(i);
                CanalEntry.Column aColumn = rowData.getAfterColumnsList().get(i);
                logColumns.add(LogColumn.builder()
                        .name(aColumn.getName())
                        .oldValue(bColumn.getValue())
                        .value(aColumn.getValue())
                        .updated(aColumn.getUpdated())
                        .build());
            }
            return logColumns;
        }
        return Collections.emptyList();
    }

    @Builder
    public static class LogInfo {
        private Class<?> clazz;
        private EntryWrapper entryWrapper;
        private long batchId;
        private long time;
    }

    @Builder
    @Getter
    private static class LogRowChange {
        private String handler;
        private Long batchId;
        private String file;
        private Long offset;
        private String schema;
        private String table;
        private CanalEntry.EventType eventType;
        private Integer count;
        private Long time;
    }

    @Builder
    @Getter
    public static class LogColumn {
        private String name;
        private String oldValue;
        private String value;
        private boolean updated;
    }
}