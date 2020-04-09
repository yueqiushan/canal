package com.fanxuankai.canal.config;

import com.alibaba.google.common.util.concurrent.ThreadFactoryBuilder;
import com.fanxuankai.canal.constants.CommonConstants;
import com.fanxuankai.canal.enums.RedisKeyPrefix;
import com.fanxuankai.canal.flow.Otter;
import com.fanxuankai.canal.metadata.EnableCanalAttributes;
import com.fanxuankai.canal.util.RedisUtils;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.fanxuankai.canal.constants.RedisConstants.CANAL_RUNNING_TAG;

/**
 * @author fanxuankai
 */
@Slf4j
public class CanalRunner implements ApplicationRunner {

    @Resource
    private RedisTemplate<String, Object> redisTemplate;
    @Resource
    private CanalConfig canalConfig;

    /**
     * 应用退出时应清除 canal running 标记
     */
    private boolean shouldClearTagWhenExit;
    /**
     * canal running Redis.key
     */
    private String key;

    private List<Otter> otters = Lists.newArrayList();
    private ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("CanalRunner").build());
    private ScheduledFuture<?> scheduledFuture;

    @Override
    public void run(ApplicationArguments args) {
        key = RedisUtils.customKey(RedisKeyPrefix.SERVICE_CACHE,
                EnableCanalAttributes.getApplicationName() + CommonConstants.SEPARATOR + CANAL_RUNNING_TAG);

        if (Objects.equals(canalConfig.getRetryStart(), Boolean.TRUE)) {
            scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(() -> {
                if (retryStart()) {
                    scheduledFuture.cancel(true);
                    scheduledExecutor.shutdown();
                }
            }, 0, canalConfig.getRetryStartIntervalSeconds(), TimeUnit.SECONDS);
        } else {
            retryStart();
        }
    }

    private boolean retryStart() {
        if (!Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(key, true))) {
            return false;
        }
        log.info("Canal 已启动, 设置标记: {}", key);
        shouldClearTagWhenExit = true;
        otters = OtterFactory.getOtters();
        otters.forEach(Otter::start);
        return true;
    }

    @PreDestroy
    public void preDestroy() {
        otters.forEach(Otter::stop);
        if (shouldClearTagWhenExit) {
            redisTemplate.delete(key);
            log.info("{} 清除标记: {}", EnableCanalAttributes.getApplicationName(), key);
        }
    }
}
