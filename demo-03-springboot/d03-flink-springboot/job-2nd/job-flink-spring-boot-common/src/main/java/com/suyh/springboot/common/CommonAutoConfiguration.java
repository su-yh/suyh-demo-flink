package com.suyh.springboot.common;

import com.suyh.springboot.common.properties.CommonProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * @author suyh
 * @since 2024-01-17
 */
@AutoConfiguration
@EnableConfigurationProperties(CommonProperties.class)
public class CommonAutoConfiguration {
}
