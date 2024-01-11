package com.suyh.d05.boot.jobmgr.config;

import com.suyh.d05.boot.jobmgr.config.properties.FlinkSpringBootProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author suyh
 * @since 2023-12-25
 */
@Configuration
@EnableConfigurationProperties(FlinkSpringBootProperties.class)
public class FlinkSpringBootConfiguration {
}
