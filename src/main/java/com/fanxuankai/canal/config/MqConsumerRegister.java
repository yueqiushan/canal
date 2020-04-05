package com.fanxuankai.canal.config;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.annotation.EnableCanalAttributes;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.TableMetadata;
import com.fanxuankai.canal.mq.MqConsumer;
import com.fanxuankai.canal.mq.MqConsumerBeanGenerator;
import com.fanxuankai.canal.mq.MqConsumerCache;
import com.fanxuankai.canal.mq.MqType;
import com.fanxuankai.canal.util.MqUtils;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;

/**
 * @author fanxuankai
 */
public class MqConsumerRegister {

    public static void registry(BeanDefinitionRegistry registry) {
        for (CanalEntityMetadata metadata : CanalEntityMetadataCache.getAllMqMetadata()) {
            TableMetadata tableMetadata = metadata.getTableMetadata();
            Class<?> typeClass = metadata.getTypeClass();
            MqConsumer<?> mqConsumer = MqConsumerCache.get(typeClass);
            if (mqConsumer == null) {
                continue;
            }
            MqType mqType = EnableCanalAttributes.getMqType();
            String topic = MqUtils.name(tableMetadata.getSchema(), tableMetadata.getName());
            if (mqType == MqType.RABBIT_MQ) {
                register(MqConsumerBeanGenerator.generateRabbitMqConsumer(typeClass, topic), mqConsumer, registry);
            } else if (mqType == MqType.XXL_MQ) {
                register(MqConsumerBeanGenerator.generateXxlMqConsumer(typeClass, topic, CanalEntry.EventType.INSERT)
                        , mqConsumer, registry);
                register(MqConsumerBeanGenerator.generateXxlMqConsumer(typeClass, topic, CanalEntry.EventType.UPDATE)
                        , mqConsumer, registry);
                register(MqConsumerBeanGenerator.generateXxlMqConsumer(typeClass, topic, CanalEntry.EventType.DELETE)
                        , mqConsumer, registry);
            }
        }
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
