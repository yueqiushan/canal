package com.fanxuankai.canal.mq;

import org.reflections.Reflections;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author fanxuankai
 */
public class MqConsumerCache {
    private static final Map<Class<?>, MqConsumer<?>> CACHE = new HashMap<>();

    public static void from(Reflections r) {
        r.getSubTypesOf(MqConsumer.class)
                .forEach(mqConsumerClass -> {
                    Type[] genericInterfaces = mqConsumerClass.getGenericInterfaces();
                    for (Type genericInterface : genericInterfaces) {
                        ParameterizedType p = (ParameterizedType) genericInterface;
                        if (!Objects.equals(p.getRawType(), MqConsumer.class)) {
                            continue;
                        }
                        Class<?> actualTypeArgument = (Class<?>) p.getActualTypeArguments()[0];
                        try {
                            CACHE.put(actualTypeArgument,
                                    mqConsumerClass.getConstructor().newInstance());
                        } catch (Exception e) {
                            throw new RuntimeException("MqConsumer 无空构造器");
                        }
                    }
                });
    }

    public static MqConsumer<?> get(Class<?> clazz) {
        return CACHE.get(clazz);
    }

}
