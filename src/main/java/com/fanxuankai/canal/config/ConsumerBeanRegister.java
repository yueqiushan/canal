package com.fanxuankai.canal.config;

import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.TableMetadata;
import com.fanxuankai.canal.mq.MqConsumer;
import com.fanxuankai.canal.mq.MqConsumerBeanGenerator;
import com.fanxuankai.canal.mq.MqConsumerCache;
import com.fanxuankai.canal.util.MqUtils;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.annotation.AnnotatedGenericBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author fanxuankai
 */
public class ConsumerBeanRegister implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        for (CanalEntityMetadata metadata : CanalEntityMetadataCache.getAllMqMetadata()) {
            TableMetadata tableMetadata = metadata.getTableMetadata();
            Class<?> typeClass = metadata.getTypeClass();
            MqConsumer<?> mqConsumer = MqConsumerCache.get(typeClass);
            if (mqConsumer == null) {
                continue;
            }
            Class<?> mqConsumerBeanClass = MqConsumerBeanGenerator.generateMqConsumer(typeClass,
                    MqUtils.name(tableMetadata.getSchema(), tableMetadata.getName()));
            AnnotatedBeanDefinition annotatedBeanDefinition = new AnnotatedGenericBeanDefinition(mqConsumerBeanClass);
            ConstructorArgumentValues cav = annotatedBeanDefinition.getConstructorArgumentValues();
            cav.addGenericArgumentValue(mqConsumer);
            BeanDefinitionHolder bh = new BeanDefinitionHolder(annotatedBeanDefinition, mqConsumerBeanClass.getName());
            BeanDefinitionReaderUtils.registerBeanDefinition(bh, registry);
        }
    }
}
