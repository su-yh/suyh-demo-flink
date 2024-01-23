package com.suyh.springboot.batch;

import com.suyh.springboot.batch.properties.BatchJobProperties;
import com.suyh.springboot.common.constants.ConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * @author suyh
 * @since 2024-01-16
 */
@ConditionalOnProperty(name = ConfigConstants.JOB_TYPE_KEY, havingValue = ConfigConstants.JOB_TYPE_BATCH)
@AutoConfiguration
@EnableConfigurationProperties(BatchJobProperties.class)
@Slf4j
public class BatchJobAutoConfiguration {
}
