package com.aiteer.springboot.stream;

import com.aiteer.springboot.common.constants.ConfigConstants;
import com.aiteer.springboot.stream.properties.StreamJobProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * @author suyh
 * @since 2024-01-16
 */
@ConditionalOnProperty(name = ConfigConstants.JOB_TYPE_KEY, havingValue = ConfigConstants.JOB_TYPE_STREAM)
@AutoConfiguration
@EnableConfigurationProperties(StreamJobProperties.class)
@Slf4j
public class StreamJobAutoConfiguration {
}
