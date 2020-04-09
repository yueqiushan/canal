package com.fanxuankai.canal.config;

import com.fanxuankai.canal.metadata.CanalEntityMetadataCache;
import com.fanxuankai.canal.metadata.EnableCanalAttributes;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author fanxuankai
 */
public class EnableCanalImportRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {

        // 主要步骤:
        // 获取 @EnableCanal 的属性
        // 扫描 @CanalEntity
        // 注测 MQ 消费者、RedisRepository 实现类

        EnableCanalAttributes.from(importingClassMetadata);

        Reflections r =
                new Reflections(new ConfigurationBuilder()
                        .forPackages(EnableCanalAttributes.getScanBasePackages())
                        .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner())
                );

        CanalEntityMetadataCache.from(r);

        BeanRegistry.registerWith(r, registry);
    }
}
