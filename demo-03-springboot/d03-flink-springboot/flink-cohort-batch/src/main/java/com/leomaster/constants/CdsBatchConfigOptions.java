package com.leomaster.constants;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author suyh
 * @since 2024-01-22
 */
public class CdsBatchConfigOptions {
    public static final ConfigOption<String> CDS_FLINK_SPRING_YAML_PATH =
            ConfigOptions.key("cds.flink.spring.batch.yaml-path")
                    .stringType()
                    .defaultValue("/opt/trend_oper/cds-job-conf.yaml")
                    .withDescription("job business config file path.");
}
