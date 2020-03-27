package com.fanxuankai.canal.wrapper;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author fanxuankai
 */
public class EntryWrapper implements Serializable {
    private CanalEntry.Entry raw;
    private CanalEntry.RowChange rowChange;
    /**
     * 可变集合
     */
    @Getter
    private List<CanalEntry.RowData> allRowDataList;

    public EntryWrapper(CanalEntry.Entry raw) {
        this.raw = raw;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(raw.getStoreValue());
            allRowDataList = new ArrayList<>(rowChange.getRowDatasList());
        } catch (Exception e) {
            throw new RuntimeException("error parse " + raw.toString());
        }
    }

    public boolean isDdl() {
        return rowChange.getIsDdl();
    }

    public String getSchemaName() {
        return raw.getHeader().getSchemaName();
    }

    public String getTableName() {
        return raw.getHeader().getTableName();
    }

    public String getLogfileName() {
        return raw.getHeader().getLogfileName();
    }

    public long getLogfileOffset() {
        return raw.getHeader().getLogfileOffset();
    }

    public CanalEntry.EventType getEventType() {
        return raw.getHeader().getEventType();
    }

    /**
     * 原始数据行数
     */
    public int getRawRowDataCount() {
        return rowChange.getRowDatasCount();
    }

}
