package com.aiteer.springboot.core.environment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.logging.DeferredLog;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

import java.util.Map;

/**
 * 在springboot 解析application.properties 相关的配置文件之前先进行相关的逻辑处理。
 */
public class FlinkSpringbootEnvironmentPostProcessor
        implements EnvironmentPostProcessor, Ordered {

    public static final DeferredLog LOGGER = new DeferredLog();

    private static volatile Map<String, Object> configProperties;

    public static void setConfigProperties(Map<String, Object> configProperties) {
        FlinkSpringbootEnvironmentPostProcessor.configProperties = configProperties;
    }

    @Override
    public int getOrder() {
        return ConfigDataEnvironmentPostProcessor.ORDER - 2;
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        MutablePropertySources propertySources = environment.getPropertySources();
        if (configProperties == null) {
            LOGGER.debug("flink springboot config properties is null.");
            return;
        }
        LOGGER.debug("flink springboot config properties size: " + configProperties.size());
        configProperties.forEach((k, v) -> LOGGER.info("flink springboot config properties, " + k + ": " + v));

        PropertySource<?> propertySource = new MapPropertySource("flink-springboot", configProperties);
        propertySources.addLast(propertySource);
    }
}
