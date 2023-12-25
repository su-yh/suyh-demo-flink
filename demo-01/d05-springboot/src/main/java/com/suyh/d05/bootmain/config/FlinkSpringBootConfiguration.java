package com.suyh.d05.bootmain.config;

import com.suyh.d05.bootmain.config.properties.FlinkSpringBootProperties;
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
