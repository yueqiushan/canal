package com.fanxuankai.canal.flow;

import com.fanxuankai.canal.wrapper.EntryWrapper;

/**
 * 处理器
 *
 * @author fanxuankai
 */
public interface Handler {

    /**
     * 是否能够处理
     *
     * @param entryWrapper 数据
     * @return true or false
     */
    boolean canHandle(EntryWrapper entryWrapper);

    /**
     * 处理
     *
     * @param entryWrapper 数据
     */
    void handle(EntryWrapper entryWrapper);
}
