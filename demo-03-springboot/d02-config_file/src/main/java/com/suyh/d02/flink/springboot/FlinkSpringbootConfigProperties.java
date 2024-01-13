package com.suyh.d02.flink.springboot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 解析yaml 文件，并转换成Map 对象
 *
 * @author suyh
 * @since 2024-01-12
 */
@Slf4j
public class FlinkSpringbootConfigProperties {

    public static Map<String, Object> parse(String yamlPath) {
        if (!StringUtils.hasText(yamlPath)) {
            log.info("There are no yaml files that need to be parsed.");
            return null;
        }

        log.info("Start parsing yaml: {}", yamlPath);

        Resource resource = new FileSystemResource(yamlPath);
        YamlPropertiesFactoryBean yamlFactory = new YamlPropertiesFactoryBean();
        yamlFactory.setResources(resource);
        Properties properties = yamlFactory.getObject();
        if (properties == null) {
            log.warn("properties result is null.");
            return null;
        }

        Map<String, Object> configProperties = new HashMap<>();
        properties.forEach((key, value) -> {
            configProperties.put(key.toString(), value);
            log.info("flink springboot yaml property, {}: {}", key, value);
        });

        return configProperties;
    }
}
