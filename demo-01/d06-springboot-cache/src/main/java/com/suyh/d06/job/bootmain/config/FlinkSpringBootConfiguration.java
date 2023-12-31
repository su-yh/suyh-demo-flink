package com.suyh.d06.job.bootmain.config;

import com.suyh.d06.job.bootmain.config.properties.FlinkSpringBootProperties;
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
