package com.fanxuankai.canal.config;

import com.fanxuankai.canal.util.App;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author fanxuankai
 */
@Configuration
@EnableConfigurationProperties(CanalConfig.class)
public class CanalAutoConfiguration {

    @Bean
    public App app() {
        return new App();
    }

    @Bean
    public CanalRunner canalRunner() {
        return new CanalRunner();
    }

}
