package com.fanxuankai.canal.config;

import com.fanxuankai.canal.redis.RedisRepository;
import com.fanxuankai.canal.redis.SimpleRedisRepository;
import com.fanxuankai.canal.util.JavassistBeanGenerator;
import org.reflections.Reflections;
import org.springframework.beans.factory.annotation.AnnotatedGenericBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * @author fanxuankai
 */
public class RedisRepositoryRegister {

    public static void registry(Reflections r, BeanDefinitionRegistry registry) {
        r.getSubTypesOf(RedisRepository.class)
                .forEach(redisRepositoryClass -> {
                    Type[] genericInterfaces = redisRepositoryClass.getGenericInterfaces();
                    for (Type genericInterface : genericInterfaces) {
                        ParameterizedType p = (ParameterizedType) genericInterface;
                        if (!Objects.equals(p.getRawType(), RedisRepository.class)
                                || Objects.equals(redisRepositoryClass, SimpleRedisRepository.class)) {
                            continue;
                        }
                        Class<?> domainType = (Class<?>) p.getActualTypeArguments()[0];
                        BeanDefinition beanDefinition =
                                new AnnotatedGenericBeanDefinition(JavassistBeanGenerator.generateRedisRepository(redisRepositoryClass, domainType));
                        BeanDefinitionHolder bh = new BeanDefinitionHolder(beanDefinition,
                                redisRepositoryClass.getName());
                        BeanDefinitionReaderUtils.registerBeanDefinition(bh, registry);
                    }
                });
    }
}
