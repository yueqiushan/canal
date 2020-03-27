package com.fanxuankai.canal.flow;

import java.util.concurrent.Flow;

/**
 * @author fanxuankai
 */
public interface Otter {

    /**
     * 订阅
     *
     * @param subscriber 订阅者
     */
    void subscribe(Flow.Subscriber<Context> subscriber);

    /**
     * 开启数据同步
     */
    void start();

    /**
     * 停止数据同步
     */
    void stop();

}
