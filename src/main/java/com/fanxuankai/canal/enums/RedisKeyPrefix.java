package com.fanxuankai.canal.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.fanxuankai.canal.constants.RedisConstants.GLOBAL_NAME;
import static com.fanxuankai.canal.constants.RedisConstants.SEPARATOR;

/**
 * @author fanxuankai
 */

@Getter
@AllArgsConstructor
public enum RedisKeyPrefix {
    // 数据库缓存
    DB_CACHE(String.format("%s%sDbCache", GLOBAL_NAME, SEPARATOR)),
    // 分布式锁
    LOCK(String.format("%s%sLock", GLOBAL_NAME, SEPARATOR)),
    // 业务缓存
    SERVICE_CACHE(String.format("%s%sServiceCache", GLOBAL_NAME, SEPARATOR)),
    ;
    private String value;
}
