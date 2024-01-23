package com.suyh.core.constants;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author suyh
 * @since 2024-01-22
 */
public class SuyhConfigOptions {
    public static final ConfigOption<String> FLINK_SPRINGBOOT_CONFIG_PATH =
            ConfigOptions.key("suyh.flink.spring.yaml-path")
                    .stringType()
                    .defaultValue("/opt/suyh_springboot_job.yaml")
                    .withDescription("job business config file path.");

}
