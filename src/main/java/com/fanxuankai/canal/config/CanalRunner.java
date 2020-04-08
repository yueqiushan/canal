package com.fanxuankai.canal.config;

import com.alibaba.google.common.util.concurrent.ThreadFactoryBuilder;
import com.fanxuankai.canal.constants.CommonConstants;
import com.fanxuankai.canal.enums.RedisKeyPrefix;
import com.fanxuankai.canal.flow.Otter;
import com.fanxuankai.canal.flow.OtterFactory;
import com.fanxuankai.canal.metadata.EnableCanalAttributes;
import com.fanxuankai.canal.mq.MqType;
import com.fanxuankai.canal.util.App;
import com.fanxuankai.canal.util.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.PreDestroy;
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

    private RedisTemplate<String, Object> redisTemplate;
    private CanalConfig canalConfig;

    /**
     * 应用退出时应清除 canal running 标记
     */
    private boolean shouldClearTagWhenExit;
    /**
     * canal running Redis.key
     */
    private String key;
    /**
     * 应用名
     */
    private String name;

    private ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("CanalRunner").build());
    private ScheduledFuture<?> scheduledFuture;

    @Override
    public void run(ApplicationArguments args) {
        redisTemplate = App.getRedisTemplate();
        canalConfig = App.getContext().getBean(CanalConfig.class);
        name = EnableCanalAttributes.getName();
        key = RedisUtils.customKey(RedisKeyPrefix.SERVICE_CACHE, name + CommonConstants.SEPARATOR + CANAL_RUNNING_TAG);

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
        if (EnableCanalAttributes.isEnableRedis()) {
            OtterFactory.getRedisOtter().ifPresent(Otter::start);
        }
        if (EnableCanalAttributes.isEnableMq()) {
            MqType type = EnableCanalAttributes.getMqType();
            if (type == MqType.RABBIT_MQ) {
                OtterFactory.getRabbitMqOtter().ifPresent(Otter::start);
            } else if (type == MqType.XXL_MQ) {
                OtterFactory.getXxlMqOtter().ifPresent(Otter::start);
            }
        }
        return true;
    }

    @PreDestroy
    public void preDestroy() {
        if (shouldClearTagWhenExit) {
            redisTemplate.delete(key);
            log.info("{} 清除标记: {}", name, key);
        }
    }
}
