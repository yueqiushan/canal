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
public class ConsumeEntryLogger {

    public static void asyncLog(LogInfo logInfo) {
        ForkJoinPool.commonPool().execute(() -> {
            EntryWrapper entryWrapper = logInfo.entryWrapper;
            LogRowChange logRowChange = LogRowChange.builder()
                    .name(logInfo.name)
                    .batchId(logInfo.batchId)
                    .time(logInfo.time)
                    .entryWrapper(entryWrapper)
                    .build();
            CanalConfig canalConfig = App.getContext().getBean(CanalConfig.class);
            if (Objects.equals(canalConfig.getShowRowChange(), Boolean.TRUE)) {
                List<List<LogColumn>> list = entryWrapper.getAllRowDataList().stream()
                        .map(o -> logColumns(o, entryWrapper.getEventType()))
                        .collect(Collectors.toList());
                log.info("{}\n{}", logRowChange, JSON.toJSONString(list,
                        Objects.equals(canalConfig.getFormatRowChangeLog(), Boolean.TRUE)));
            } else {
                log.info("{}", logRowChange);
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
        private String name;
        private EntryWrapper entryWrapper;
        private long batchId;
        private long time;
    }

    @Builder
    private static class LogRowChange {
        private String name;
        private Long batchId;
        private Long time;

        private EntryWrapper entryWrapper;

        @Override
        public String toString() {
            return String.format("%s.%s %s.%s.%s, batchId: %s, count: %s, time: %sms %s",
                    entryWrapper.getLogfileName(), entryWrapper.getLogfileOffset(), entryWrapper.getSchemaName(),
                    entryWrapper.getTableName(), entryWrapper.getEventType(), batchId,
                    entryWrapper.getRawRowDataCount(), time, name);
        }
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
