package com.aiteer.springboot.common.properties;

import com.aiteer.springboot.common.constants.ConfigConstants;
import com.aiteer.springboot.common.vo.CohortThreadPoolProperties;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.validation.annotation.Validated;

/**
 * @author suyh
 * @since 2024-01-16
 */
@ConfigurationProperties(prefix = ConfigConstants.CDS_PROP_PREFIX_COMMON)
@Data
@Validated
public class CommonProperties {

    @NestedConfigurationProperty
    private final CohortThreadPoolProperties cohortThreadPool = new CohortThreadPoolProperties();
}
