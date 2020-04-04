package com.fanxuankai.canal.flow;

/**
 * 处理器接口
 *
 * @author fanxuankai
 */
public interface Handler<T> {
    /**
     * 处理
     *
     * @param t 数据
     */
    void handle(T t);
}
