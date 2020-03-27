package com.fanxuankai.canal.flow;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.aviator.Aviators;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.apache.commons.lang3.StringUtils;

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

    protected static void filterEntryRowData(EntryWrapper entryWrapper,
                                             Function<CanalEntityMetadata, String> aviatorExpressionFunction,
                                             boolean filterBeforeColumn) {
        CanalEntityMetadata metadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        String aviatorExpression = aviatorExpressionFunction.apply(metadata);
        if (StringUtils.isBlank(aviatorExpression)) {
            return;
        }
        Class<?> typeClass = metadata.getTypeClass();
        entryWrapper.getAllRowDataList()
                .removeIf(rowData -> {
                    List<CanalEntry.Column> columnsList;
                    if (filterBeforeColumn) {
                        columnsList = rowData.getBeforeColumnsList();
                    } else {
                        columnsList = rowData.getAfterColumnsList();
                    }
                    return !Aviators.exec(columnsList, aviatorExpression, typeClass);
                });
    }

    protected static String json(List<CanalEntry.Column> columns) {
        return JSON.toJSONString(columns.stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                CanalEntry.Column::getValue)));
    }

    protected static String json(List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns) {
        Map<String, String> map0 = beforeColumns.stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                CanalEntry.Column::getValue));
        Map<String, String> map1 = afterColumns.stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                CanalEntry.Column::getValue));
        List<Map<String, String>> list = new ArrayList<>(2);
        list.add(map0);
        list.add(map1);
        return JSON.toJSONString(list);
    }
}
