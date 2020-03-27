package com.fanxuankai.canal.config;

import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

/**
 * @author fanxuankai
 */
@Configuration
public class TableMetadataConfig implements InitializingBean {
    @Resource
    private CanalConfig canalConfig;

    @Override
    public void afterPropertiesSet() {
        setDefaultSchemaIfNot();
    }

    private void setDefaultSchemaIfNot() {
        CanalEntityMetadataCache.setDefaultSchema(canalConfig.getSchema());
    }
}
