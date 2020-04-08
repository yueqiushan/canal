package com.fanxuankai.canal.config;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.CanalEntityMetadataCache;
import com.fanxuankai.canal.metadata.EnableCanalAttributes;
import com.fanxuankai.canal.metadata.TableMetadata;
import com.fanxuankai.canal.mq.MqConsumer;
import com.fanxuankai.canal.mq.MqType;
import com.fanxuankai.canal.redis.RedisRepository;
import com.fanxuankai.canal.redis.SimpleRedisRepository;
import com.fanxuankai.canal.util.JavassistBeanGenerator;
import com.fanxuankai.canal.util.QueueNameUtils;
import com.google.common.collect.Maps;
import org.reflections.Reflections;
import org.springframework.beans.factory.annotation.AnnotatedGenericBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;

/**
 * @author fanxuankai
 */
public class BeanRegistry {

    /**
     * 注册 bean
     *
     * @param r        Reflections 对象
     * @param registry BeanDefinitionRegistry
     */
    public static void registerWith(Reflections r, BeanDefinitionRegistry registry) {
        registerMqConsumer(r, registry);
        registerRedisRepository(r, registry);
    }

    /**
     * 自动生成 MQ 消费者且注册为 Spring bean
     *
     * @param r        Reflections 对象
     * @param registry BeanDefinitionRegistry
     */
    private static void registerMqConsumer(Reflections r, BeanDefinitionRegistry registry) {
        // key: domainType value: MqConsumer
        Map<Class<?>, MqConsumer<?>> consumerByDomainType = Maps.newHashMap();
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
                            consumerByDomainType.put(actualTypeArgument,
                                    mqConsumerClass.getConstructor().newInstance());
                        } catch (Exception e) {
                            throw new RuntimeException("MqConsumer 无空构造器");
                        }
                    }
                });
        for (CanalEntityMetadata metadata : CanalEntityMetadataCache.getAllMqMetadata()) {
            TableMetadata tableMetadata = metadata.getTableMetadata();
            Class<?> domainType = metadata.getDomainType();
            MqConsumer<?> mqConsumer = consumerByDomainType.get(domainType);
            if (mqConsumer == null) {
                continue;
            }
            MqType mqType = EnableCanalAttributes.getMqType();
            String topic = QueueNameUtils.name(tableMetadata.getSchema(), tableMetadata.getName());
            if (mqType == MqType.RABBIT_MQ) {
                register(JavassistBeanGenerator.generateRabbitMqConsumer(domainType, topic), mqConsumer, registry);
            } else if (mqType == MqType.XXL_MQ) {
                register(JavassistBeanGenerator.generateXxlMqConsumer(domainType, topic, CanalEntry.EventType.INSERT)
                        , mqConsumer, registry);
                register(JavassistBeanGenerator.generateXxlMqConsumer(domainType, topic, CanalEntry.EventType.UPDATE)
                        , mqConsumer, registry);
                register(JavassistBeanGenerator.generateXxlMqConsumer(domainType, topic, CanalEntry.EventType.DELETE)
                        , mqConsumer, registry);
            }
        }
    }

    /**
     * 自动实现自定的 RedisRepository 接口的实现类且注册为 Spring bean
     *
     * @param r        Reflections 对象
     * @param registry BeanDefinitionRegistry
     */
    private static void registerRedisRepository(Reflections r, BeanDefinitionRegistry registry) {
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
                        BeanDefinition beanDefinition = new AnnotatedGenericBeanDefinition(
                                JavassistBeanGenerator.generateRedisRepository(redisRepositoryClass, domainType));
                        BeanDefinitionHolder bh = new BeanDefinitionHolder(beanDefinition,
                                redisRepositoryClass.getName());
                        BeanDefinitionReaderUtils.registerBeanDefinition(bh, registry);
                    }
                });
    }

    private static void register(Class<?> mqConsumerBeanClass, MqConsumer<?> mqConsumer,
                                 BeanDefinitionRegistry registry) {
        RootBeanDefinition beanDefinition = new RootBeanDefinition(mqConsumerBeanClass);
        ConstructorArgumentValues cav = beanDefinition.getConstructorArgumentValues();
        cav.addGenericArgumentValue(mqConsumer);
        BeanDefinitionHolder bh = new BeanDefinitionHolder(beanDefinition, mqConsumerBeanClass.getName());
        BeanDefinitionReaderUtils.registerBeanDefinition(bh, registry);
    }
}
