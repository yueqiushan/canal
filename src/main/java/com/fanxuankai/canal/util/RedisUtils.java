package com.fanxuankai.canal.util;

import com.fanxuankai.canal.enums.RedisKeyPrefix;
import org.apache.commons.lang.StringUtils;

import static com.fanxuankai.canal.constants.CommonConstants.SEPARATOR;
import static com.fanxuankai.canal.enums.RedisKeyPrefix.DB_CACHE;

/**
 * redis 工具类
 *
 * @author fanxuankai
 */
public class RedisUtils {

    /**
     * 生成 key
     *
     * @param schema 数据库名
     * @param table  表名
     * @return 生成默认的 key
     */
    public static String key(String schema, String table) {
        return key(schema, table, null);
    }

    /**
     * 生成 key
     *
     * @param schema 数据库名
     * @param table  表名
     * @param suffix 后缀
     * @return 生成默认的 key
     */
    public static String key(String schema, String table, String suffix) {
        String key = DB_CACHE.getValue() + SEPARATOR + schema + SEPARATOR + table;
        if (StringUtils.isNotEmpty(suffix)) {
            return key + SEPARATOR + suffix;
        }
        return key;
    }

    /**
     * 生成 key
     *
     * @param suffix 后缀
     * @return 生成自定义的 key
     */
    public static String customKey(String key, String suffix) {
        return key + SEPARATOR + suffix;
    }

    /**
     * 生成 key
     *
     * @param prefix 后缀
     * @param custom 自定义
     * @return 生成自定义的 key
     */
    public static String customKey(RedisKeyPrefix prefix, String custom) {
        return prefix.getValue() + SEPARATOR + custom;
    }
}
