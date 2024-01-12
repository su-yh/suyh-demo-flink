package com.suyh.springboot.constants;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * 配置常量
 * @author suyh
 * @since 2024-01-12
 */
public class DemoConfigConstants {
    public static final ConfigOption<String> SUYH_CFG =
            ConfigOptions.key("suyh.state.cfg")
                    // 配置项的类型
                    .stringType()
                    // 配置项的默认值
                    .defaultValue("suyh-default-value")
                    // 已经过期的key
                    .withDeprecatedKeys("suyh.state.old-cfg")
                    // 对该配置的描述信息
                    .withDescription(
                            "The default directory for savepoints. Used by the state backends that write savepoints to"
                                    + " file systems (HashMapStateBackend, EmbeddedRocksDBStateBackend).");

}
