package com.fanxuankai.canal.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * @author fanxuankai
 */
public class DomainConverter {

    private static final int DEFAULT_JSON_LIST_SIZE = 2;

    /**
     * 用于 insert、delete 时
     *
     * @param json   json 字符串
     * @param tClass 要转换的类型 class
     * @param <T>    要转换的类型
     * @return 转换后的运行时对象
     */
    public static <T> T of(String json, Class<T> tClass) {
        return JSON.parseObject(json, tClass);
    }

    /**
     * 用于 update
     *
     * @param json   json 字符串
     * @param tClass 要转换的类型 class
     * @param <T>    要转换的类型
     * @return 转换后的运行时对象
     */
    public static <T> Pair<T, T> pairOf(String json, Class<T> tClass) {
        List<T> ts = JSON.parseArray(json, tClass);
        if (ts == null || ts.size() != DEFAULT_JSON_LIST_SIZE) {
            return Pair.of(null, null);
        }
        return Pair.of(ts.get(0), ts.get(1));
    }

    public static void main(String[] args) {
        Pair<Long, Long> p = pairOf("", Long.class);
        p.getLeft();
        p.getRight();

    }

}
