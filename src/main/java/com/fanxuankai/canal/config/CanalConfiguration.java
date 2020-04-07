package com.fanxuankai.canal.config;

import com.fanxuankai.canal.metadata.CanalEntityMetadataCache;
import com.fanxuankai.canal.metadata.EnableCanalAttributes;
import com.fanxuankai.canal.util.App;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.beans.factory.annotation.AnnotatedGenericBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author fanxuankai
 */
public class CanalConfiguration implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {

        // 主要步骤:
        // ApplicationContext
        // 注册 CanalConfig
        // 获取 EnableCanal
        // 扫描 CanalEntity
        // 生成 RedisRepository 实现类
        // 生成 MQ 消费者类
        // 注册 OtterRunner

        registry.registerBeanDefinition(App.class.getName(), new AnnotatedGenericBeanDefinition(App.class));

        registry.registerBeanDefinition(CanalConfig.class.getName(),
                new AnnotatedGenericBeanDefinition(CanalConfig.class));

        EnableCanalAttributes.from(importingClassMetadata);

        Reflections r =
                new Reflections(new ConfigurationBuilder()
                        .forPackages(EnableCanalAttributes.getScanBasePackages())
                        .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner())
                );

        CanalEntityMetadataCache.from(r);

        RedisRepositoryRegister.registry(r, registry);

        MqConsumerRegister.registry(r, registry);

        registry.registerBeanDefinition(CanalRunner.class.getName(), new RootBeanDefinition(CanalRunner.class));
    }
}
