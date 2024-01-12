package com.suyh.d02.springboot.environment;

import com.suyh.d02.flink.constants.FlinkSpringbootConfigConstants;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ReadableConfig;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author suyh
 * @since 2024-01-12
 */
@Slf4j
public class FlinkSpringbootConfigProperties {
    private static FlinkSpringbootConfigProperties instance;
    private static final ReentrantLock LOCK = new ReentrantLock();

    @Getter
    private Map<String, Object> propertySource;

    public static FlinkSpringbootConfigProperties getInstance() {
        if (instance == null) {
            try {
                LOCK.lock();
                if (instance == null) {
                    instance = new FlinkSpringbootConfigProperties();
                }
            } finally {
                LOCK.unlock();
            }
        }

        return instance;
    }

    public void init(ReadableConfig configuration) {
        String filePath = configuration.get(FlinkSpringbootConfigConstants.FLINK_SPRINGBOOT_CONFIG_FILE);
        if (!StringUtils.hasText(filePath)) {
            log.info("no config key: {}", FlinkSpringbootConfigConstants.FLINK_SPRINGBOOT_CONFIG_FILE);
            return;
        }

        Resource resource = new FileSystemResource(filePath);
        YamlPropertiesFactoryBean yamlFactory = new YamlPropertiesFactoryBean();
        yamlFactory.setResources(resource);
        Properties properties = yamlFactory.getObject();
        if (properties == null) {
            log.warn("properties result is null.");
            return;
        }

        loadPropertySource(properties);
    }

    // 禁止并发访问
    private synchronized void loadPropertySource(Properties properties) {
        propertySource = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            propertySource.put(key, properties.getProperty(key));
        }
    }
}
