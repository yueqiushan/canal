package com.fanxuankai.canal.config;

import com.fanxuankai.canal.flow.Otter;
import com.fanxuankai.canal.metadata.EnableCanalAttributes;
import com.fanxuankai.canal.mq.MqType;
import com.fanxuankai.canal.mq.RabbitMqFlowOtter;
import com.fanxuankai.canal.mq.XxlMqFlowOtter;
import com.fanxuankai.canal.redis.RedisFlowOtter;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Otter 工厂
 *
 * @author fanxuankai
 */
public class OtterFactory {

    /**
     * 根据 @EnableCanal 创建 Otter 客户端
     *
     * @return List<Otter> NotNull
     */
    public static List<Otter> getOtters() {
        List<Otter> otters = Lists.newArrayList();
        if (EnableCanalAttributes.isEnableRedis()) {
            otters.add(new RedisFlowOtter());
        }
        if (EnableCanalAttributes.isEnableMq()) {
            MqType type = EnableCanalAttributes.getMqType();
            if (type == MqType.RABBIT_MQ) {
                otters.add(new RabbitMqFlowOtter());
            } else if (type == MqType.XXL_MQ) {
                otters.add(new XxlMqFlowOtter());
            }
        }
        return otters;
    }

}
