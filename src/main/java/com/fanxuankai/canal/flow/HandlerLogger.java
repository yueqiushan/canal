package com.fanxuankai.canal.flow;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.util.ThreadPoolService;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

/**
 * @author fanxuankai
 */
@Slf4j
public class HandlerLogger {

    public static void asyncLog(LogInfo logInfo) {
        ThreadPoolService.getInstance().execute(() -> {
            EntryWrapper entryWrapper = logInfo.entryWrapper;
            LogRowChange build = LogRowChange.builder()
                    .handler(logInfo.handler.getClass().getName())
                    .batchId(logInfo.batchId)
                    .file(entryWrapper.getLogfileName())
                    .offset(entryWrapper.getLogfileOffset())
                    .schema(entryWrapper.getSchemaName())
                    .table(entryWrapper.getTableName())
                    .eventType(entryWrapper.getEventType())
                    .count(entryWrapper.getAllRowDataList().size())
                    .time(logInfo.time)
                    .build();
            log.info(JSON.toJSONString(build));
            if (logInfo.getCanalConfig().isShowRowChange()) {
                log.info(JSON.toJSONString(entryWrapper.getAllRowDataList().stream().map(o -> logColumns(o,
                        entryWrapper.getEventType())), logInfo.getCanalConfig().isFormatRowChangeLog()));
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

    @Data
    @Builder
    public static class LogInfo {
        private CanalConfig canalConfig;
        private Handler handler;
        private EntryWrapper entryWrapper;
        private long batchId;
        private long time;
    }

    @Builder
    @Data
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

    @Data
    @Builder
    public static class LogColumn {
        private String name;
        private String oldValue;
        private String value;
        private boolean updated;
    }
}
