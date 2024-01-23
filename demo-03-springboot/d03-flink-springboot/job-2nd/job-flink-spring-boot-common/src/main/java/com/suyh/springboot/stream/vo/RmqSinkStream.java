package com.suyh.springboot.stream.vo;

import com.suyh.springboot.common.vo.RmqConnectProperties;
import lombok.Data;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import javax.validation.Valid;

/**
 * @author suyh
 * @since 2024-01-18
 */
@Data
public class RmqSinkStream {
    @NestedConfigurationProperty
    @Valid
    private final RmqConnectProperties connect = new RmqConnectProperties();

    private String exchange;

    private String routingKey;
    private String routingKeyPaidRetention;
}
