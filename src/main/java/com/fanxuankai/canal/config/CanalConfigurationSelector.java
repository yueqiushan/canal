package com.fanxuankai.canal.config;

import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.annotation.EnableCanalAttributes;
import com.fanxuankai.canal.util.App;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author fanxuankai
 */
public class CanalConfigurationSelector implements ImportSelector {

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        EnableCanalAttributes.from(importingClassMetadata);
        CanalEntityMetadataCache.load();
        // 使用 EnableCanal 注解时, 导入所需的 Bean
        return new String[]{App.class.getName(), CanalConfig.class.getName(),
                CanalConfigurationImporter.class.getName(),
                ConsumerBeanRegister.class.getName()};
    }
}
