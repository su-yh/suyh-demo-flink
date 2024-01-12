package com.suyh.d02.springboot.environment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.env.EnvironmentPostProcessor;
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

    @Override
    public int getOrder() {
        // 配置中心的属性配置优先级需要高于本地属性配置
        // return ConfigFileApplicationListener.DEFAULT_ORDER - 2; // springboot 2.4 版本之前
        return ConfigDataEnvironmentPostProcessor.ORDER - 2;
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        // 在启动时，这里首次加载配置中心。将配置中心的值加载下来并放到 environment 的属性源中。
        // 并按顺序放在系统属性源之后。
        MutablePropertySources propertySources = environment.getPropertySources();
        Map<String, Object> configProperties = FlinkSpringbootConfigProperties.getInstance().getPropertySource();
        if (configProperties == null) {
            System.out.println("flink springboot config properties is null.");
            return;
        }
        System.out.println("flink springboot config properties size: " + configProperties.size());

        PropertySource<?> propertySource = new MapPropertySource("flink-springboot", configProperties);
        propertySources.addLast(propertySource);
    }
}
