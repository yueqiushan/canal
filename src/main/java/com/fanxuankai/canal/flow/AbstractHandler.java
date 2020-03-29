package com.fanxuankai.canal.flow;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.aviator.Aviators;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.FilterMetadata;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 抽象处理器
 *
 * @author fanxuankai
 */
public abstract class AbstractHandler implements Handler {

    protected void filterEntryRowData(EntryWrapper entryWrapper,
                                      Function<CanalEntityMetadata, FilterMetadata> filterMetadataFunction,
                                      boolean filterBeforeColumn) {
        CanalEntityMetadata metadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        FilterMetadata filterMetadata = filterMetadataFunction.apply(metadata);
        Class<?> typeClass = metadata.getTypeClass();
        entryWrapper.getAllRowDataList()
                .removeIf(rowData -> {
                    List<CanalEntry.Column> columnsList;
                    if (filterBeforeColumn) {
                        columnsList = rowData.getBeforeColumnsList();
                    } else {
                        columnsList = rowData.getAfterColumnsList();
                    }
                    return shouldRemove(columnsList, filterMetadata, typeClass);
                });
    }

    protected String json(List<CanalEntry.Column> columns) {
        return JSON.toJSONString(columns.stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                CanalEntry.Column::getValue)));
    }

    protected String json(List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns) {
        Map<String, String> map0 = beforeColumns.stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                CanalEntry.Column::getValue));
        Map<String, String> map1 = afterColumns.stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                CanalEntry.Column::getValue));
        List<Map<String, String>> list = new ArrayList<>(2);
        list.add(map0);
        list.add(map1);
        return JSON.toJSONString(list);
    }

    private boolean shouldRemove(List<CanalEntry.Column> columnList, FilterMetadata filterMetadata,
                                 Class<?> typeClass) {
        List<String> updatedFields = filterMetadata.getUpdatedFields();
        if (!CollectionUtils.isEmpty(updatedFields)) {
            boolean allMatch = columnList.stream()
                    .filter(column -> updatedFields.contains(column.getName()))
                    .allMatch(CanalEntry.Column::getUpdated);
            if (!allMatch) {
                return true;
            }
        }
        String aviatorExpression = filterMetadata.getAviatorExpression();
        if (StringUtils.isNotBlank(aviatorExpression)) {
            return !Aviators.exec(columnList, aviatorExpression, typeClass);
        }
        return false;
    }
}
