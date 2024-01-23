package com.aiteer.springboot.common.vo;

import lombok.Data;

import javax.validation.constraints.NotNull;

/**
 * @author suyh
 * @since 2024-01-17
 */
@Data
public class RmqConnectProperties {
    private String host = "localhost";
    private Integer port = 5672;
    @NotNull
    private String userName;
    @NotNull
    private String password;
    private String virtualHost;
}
