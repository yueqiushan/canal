package com.fanxuankai.canal.config;

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

import static com.fanxuankai.canal.constants.RedisConstants.CANAL_RUNNING_TAG;

/**
 * @author fanxuankai
 */
@Slf4j
public class CanalRunner implements ApplicationRunner {

    private RedisTemplate<String, Object> redisTemplate;

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

    @Override
    public void run(ApplicationArguments args) {

        redisTemplate = App.getRedisTemplate();
        name = EnableCanalAttributes.getName();
        key = RedisUtils.customKey(RedisKeyPrefix.SERVICE_CACHE, name + CommonConstants.SEPARATOR + CANAL_RUNNING_TAG);

        if (!Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(key, true))) {
            log.info("{} 已有实例建立了 Canal 连接", name);
            return;
        }

        log.info("{} 设置标记: {}", name, key);
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
    }

    @PreDestroy
    public void preDestroy() {
        if (shouldClearTagWhenExit) {
            redisTemplate.delete(key);
            log.info("{} 清除标记: {}", name, key);
        }
    }
}
