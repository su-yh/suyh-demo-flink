package com.suyh.core.util;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.File;
import java.util.Map;
import java.util.Properties;


/**
 * @author suyh
 * @since 2024-01-22
 */
public class FileLoadUtils {
    public static void yamlConfigProperties(String yamlFilePath, Map<String, Object> configProperties) {
        System.out.println("yaml config file path: " + yamlFilePath);
        File file = new File(yamlFilePath);
        if (!file.exists()) {
            System.out.println("WARNING! file not exists: " + yamlFilePath);
        } else {
            YamlPropertiesFactoryBean yamlFactory = new YamlPropertiesFactoryBean();
            Resource resource = new FileSystemResource(yamlFilePath);

            yamlFactory.setResources(resource);
            Properties properties = yamlFactory.getObject();
            if (properties == null || properties.size() <= 0) {
                System.out.println("file config size is empty. path: " + yamlFilePath);
            } else {
                properties.forEach((key, value) -> {
                    configProperties.put(key.toString(), value);
                    System.out.println("file config, " + key + ": " + value);
                });
            }
        }
    }
}
