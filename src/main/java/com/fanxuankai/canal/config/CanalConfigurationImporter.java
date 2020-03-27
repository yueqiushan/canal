package com.fanxuankai.canal.config;

import com.fanxuankai.canal.annotation.EnableCanalAttributes;
import com.fanxuankai.canal.enums.RedisKeyPrefix;
import com.fanxuankai.canal.flow.Otter;
import com.fanxuankai.canal.flow.OtterFactory;
import com.fanxuankai.canal.mq.MqType;
import com.fanxuankai.canal.util.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import static com.fanxuankai.canal.constants.RedisConstants.CANAL_RUNNING_TAG;
import static com.fanxuankai.canal.constants.RedisConstants.SEPARATOR;

/**
 * @author fanxuankai
 */
@Slf4j
public class CanalConfigurationImporter implements ApplicationRunner {
    @Resource
    private RedisTemplate<String, Object> redisTemplate;
    @Resource
    private CanalConfig canalConfig;
    @Resource
    private AmqpTemplate amqpTemplate;

    /**
     * 应用退出时应清除 canal running 标记
     */
    private boolean showClearTagWhenExit;
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
        name = EnableCanalAttributes.getName();
        key = RedisUtil.customKey(RedisKeyPrefix.SERVICE_CACHE, name + SEPARATOR + CANAL_RUNNING_TAG);
        Boolean setCanalRunning = redisTemplate.opsForValue().setIfAbsent(key, true);
        if (!Boolean.TRUE.equals(setCanalRunning)) {
            log.info("{} 已有实例建立了 Canal 连接", name);
            return;
        }
        log.info("{} 设置标记: {}", name, key);
        showClearTagWhenExit = true;
        if (EnableCanalAttributes.isEnableRedis()) {
            OtterFactory.getRedisOtter(canalConfig, redisTemplate).ifPresent(Otter::start);
        }
        if (EnableCanalAttributes.isEnableMq()) {
            MqType type = EnableCanalAttributes.getMqType();
            if (type == MqType.RABBIT_MQ) {
                OtterFactory.getRabbitMqOtter(canalConfig, amqpTemplate, redisTemplate).ifPresent(Otter::start);
            } else if (type == MqType.XXL_MQ) {
                OtterFactory.getXxlMqOtter(canalConfig, redisTemplate).ifPresent(Otter::start);
            }
        }

    }

    @PreDestroy
    public void preDestroy() {
        if (showClearTagWhenExit) {
            redisTemplate.delete(key);
            log.info("{} 清除标记: {}", name, key);
        }
    }
}
