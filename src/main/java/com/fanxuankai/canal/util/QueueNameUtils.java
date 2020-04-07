package com.fanxuankai.canal.util;

import com.alibaba.otter.canal.protocol.CanalEntry;

import static com.fanxuankai.canal.constants.CommonConstants.SEPARATOR;
import static com.fanxuankai.canal.constants.QueuePrefixConstants.CANAL_2_MQ;

/**
 * MQ 队列名工具类
 *
 * @author fanxuankai
 */
public class QueueNameUtils {
    public static String name(String schema, String table) {
        return CANAL_2_MQ + SEPARATOR + schema + SEPARATOR + table;
    }

    public static String name(String schema, String table, CanalEntry.EventType eventType) {
        return CANAL_2_MQ + SEPARATOR + schema + SEPARATOR + table + SEPARATOR + eventType;
    }

    public static String customName(String queue, CanalEntry.EventType eventType) {
        return CANAL_2_MQ + SEPARATOR + queue + SEPARATOR + eventType;
    }
}
