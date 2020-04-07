package com.fanxuankai.canal.util;

import com.alibaba.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池, 枚举方式实现单例模式
 *
 * @author fanxuankai
 */
@Slf4j
public class ThreadPoolService {

    private ThreadPoolService() {
    }

    public static ExecutorService getInstance() {
        return Singleton.INSTANCE.executorService;
    }

    private enum Singleton {
        // 实例
        INSTANCE;

        private ExecutorService executorService;

        Singleton() {
            executorService = threadPoolExecutor();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> executorService.shutdown()));
        }

        private ThreadPoolExecutor threadPoolExecutor() {
            return new ThreadPoolExecutor(20, 90, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("canal-%d").build());
        }
    }

}
