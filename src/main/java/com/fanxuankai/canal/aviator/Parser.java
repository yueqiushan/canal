package com.fanxuankai.canal.aviator;

/**
 * 解析字符串
 *
 * @author fanxuankai
 */
public interface Parser<T> {
    /**
     * 解析
     *
     * @param s 字符串
     * @return 解析后的类型
     */
    T parser(String s);
}
