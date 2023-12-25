package com.suyh.d06.bootmain.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author suyh
 * @since 2023-12-25
 */
@Data
@ConfigurationProperties(prefix = FlinkSpringBootProperties.PREFIX)
public class FlinkSpringBootProperties {
    public static final String PREFIX = "suyh.flink";

    private Integer parallelism;
}
