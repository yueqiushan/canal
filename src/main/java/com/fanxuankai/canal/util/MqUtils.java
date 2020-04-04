package com.fanxuankai.canal.util;

import static com.fanxuankai.canal.constants.CommonConstants.SEPARATOR;
import static com.fanxuankai.canal.constants.QueuePrefixConstants.CANAL_2_MQ;

/**
 * @author fanxuankai
 */
public class MqUtils {
    public static String name(String schema, String table) {
        return CANAL_2_MQ + SEPARATOR + schema + SEPARATOR + table;
    }

    public static String name(String schema, String table, String eventType) {
        return CANAL_2_MQ + SEPARATOR + schema + SEPARATOR + table + SEPARATOR + eventType;
    }

    public static String customName(String queue, String eventType) {
        return CANAL_2_MQ + SEPARATOR + queue + SEPARATOR + eventType;
    }
}
