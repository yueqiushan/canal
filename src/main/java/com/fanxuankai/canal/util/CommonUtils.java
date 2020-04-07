package com.fanxuankai.canal.util;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 公共工具类
 *
 * @author fanxuankai
 */
public class CommonUtils {
    public static Map<String, String> toMap(List<CanalEntry.Column> columnList) {
        return columnList.stream()
                .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
    }

    public static Map<String, CanalEntry.Column> toColumnMap(List<CanalEntry.Column> columnList) {
        return columnList.stream()
                .collect(Collectors.toMap(CanalEntry.Column::getName, o -> o));
    }
}
