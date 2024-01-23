package com.suyh.d02.app.constants;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * 配置常量
 * @author suyh
 * @since 2024-01-12
 */
public class FlinkSpringbootConfigConstants {
    public static final ConfigOption<String> FLINK_SPRINGBOOT_CONFIG_FILE =
            ConfigOptions.key("suyh.flink.springboot.config.file")
                    // 配置项的类型
                    .stringType()
                    // 配置项的默认值
//                     .defaultValue("/opt/application-json.yaml")
                    .noDefaultValue()
                    // 已经过期的key
                    // .withDeprecatedKeys("suyh.state.old-cfg")
                    // 对该配置的描述信息
                    .withDescription(
                            "flink 集成springboot，提供外置的application 配置文件，以高于打包中的resources 下的配置。");

}
