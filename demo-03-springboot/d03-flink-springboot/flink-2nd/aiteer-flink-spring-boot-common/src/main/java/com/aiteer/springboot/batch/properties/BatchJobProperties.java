package com.aiteer.springboot.batch.properties;

import com.aiteer.springboot.batch.vo.RmqSinkBatch;
import com.aiteer.springboot.common.constants.ConfigConstants;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

/**
 * @author suyh
 * @since 2024-01-16
 */
@ConfigurationProperties(prefix = ConfigConstants.CDS_PROP_PREFIX_BATCH)
@Data
@Validated
public class BatchJobProperties {
    @Valid
    private final CdsRmq cdsRmq = new CdsRmq();

    @Data
    public static class CdsRmq {
        @NestedConfigurationProperty
        @Valid
        private final RmqSinkBatch sink = new RmqSinkBatch();
    }
}
