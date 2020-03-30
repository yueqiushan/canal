package com.fanxuankai.canal.flow;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.aviator.Aviators;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.FilterMetadata;
import com.fanxuankai.canal.util.CommonUtils;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 抽象处理器
 *
 * @author fanxuankai
 */
public abstract class AbstractHandler implements Handler {

    /**
     * 是否能够处理
     *
     * @param entryWrapper 数据
     * @return true or false
     */
    protected abstract boolean canHandle(EntryWrapper entryWrapper);

    /**
     * 过滤
     *
     * @param metadata CanalEntity 注解元数据
     * @return Filter 注解元数据
     */
    protected abstract FilterMetadata filter(CanalEntityMetadata metadata);

    /**
     * 处理
     *
     * @param entryWrapper 数据
     */
    protected abstract void doHandle(EntryWrapper entryWrapper);

    @Override
    public void handle(EntryWrapper entryWrapper) {
        if (canHandle(entryWrapper)) {
            CanalEntityMetadata metadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
            FilterMetadata filterMetadata = filter(metadata);
            filterEntryRowData(entryWrapper, metadata, filterMetadata);
            doHandle(entryWrapper);
        }
    }

    protected String json(List<CanalEntry.Column> columnList) {
        return JSON.toJSONString(CommonUtils.toMap(columnList));
    }

    protected String json(List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns) {
        Map<String, String> map0 = CommonUtils.toMap(beforeColumns);
        Map<String, String> map1 = CommonUtils.toMap(afterColumns);
        List<Object> list = new ArrayList<>(2);
        list.add(map0);
        list.add(map1);
        return new JSONArray(list).toJSONString();
    }

    private void filterEntryRowData(EntryWrapper entryWrapper,
                                    CanalEntityMetadata metadata,
                                    FilterMetadata filterMetadata) {
        List<CanalEntry.RowData> rowDataList =
                entryWrapper.getAllRowDataList()
                        .stream()
                        .filter(rowData -> filterRowData(rowData, filterMetadata, metadata.getTypeClass()))
                        .collect(Collectors.toList());
        entryWrapper.setAllRowDataList(rowDataList);
    }

    private boolean filterRowData(CanalEntry.RowData rowData, FilterMetadata filterMetadata, Class<?> typeClass) {
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
                return Aviators.exec(CommonUtils.toMap(rowData.getAfterColumnsList()), aviatorExpression, typeClass);
            }
            // 删除
            if (!CollectionUtils.isEmpty(beforeColumnMap)) {
                return Aviators.exec(CommonUtils.toMap(rowData.getBeforeColumnsList()), aviatorExpression, typeClass);
            }
        }
        return true;
    }
}
