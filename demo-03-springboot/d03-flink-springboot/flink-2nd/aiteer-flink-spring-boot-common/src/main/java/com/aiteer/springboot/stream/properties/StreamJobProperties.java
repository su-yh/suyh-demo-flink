package com.aiteer.springboot.stream.properties;

import com.aiteer.springboot.common.constants.ConfigConstants;
import com.aiteer.springboot.common.constants.ConstantUtils;
import com.aiteer.springboot.common.vo.RmqConnectProperties;
import com.aiteer.springboot.stream.vo.RmqSinkStream;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

/**
 * @author suyh
 * @since 2024-01-16
 */
@ConfigurationProperties(prefix = ConfigConstants.CDS_PROP_PREFIX_STREAM)
@Data
@Validated
public class StreamJobProperties {
    @Valid
    private final CdsRmq cdsRmq = new CdsRmq();

    @Valid
    private final CdsRuntime cdsRuntime = new CdsRuntime();

    @Data
    public static class CdsRuntime {
        private int timeTriggerSecond = ConstantUtils.STAT_WINDOW_TRIGGER_SECS * ConstantUtils.STAT_WINDOW_TRIGGER_SECS_TIME;
    }

    @Data
    public static class CdsRmq {
        @Valid
        private final SourceRmq source = new SourceRmq();

        @NestedConfigurationProperty
        @Valid
        private final RmqSinkStream sink = new RmqSinkStream();
    }

    @Data
    public static class SourceRmq {
        @NestedConfigurationProperty
        @Valid
        private final RmqConnectProperties connect = new RmqConnectProperties();

        private Integer prefetchCount = 0;

        private String queueUserRegistry = ConstantUtils.POLY_TB_USER;
        private String queueUserLogin = ConstantUtils.POLY_TB_USER_LOGIN;
        private String queueUserRecharge = ConstantUtils.POLY_TB_RECHARGE;
        private String queueUserWithdrawal = ConstantUtils.POLY_TB_WITHDRAWAL;
    }
}
