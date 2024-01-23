package com.leomaster;

import com.aiteer.springboot.common.constants.ConfigConstants;
import com.aiteer.springboot.core.context.FlinkSpringContext;
import com.aiteer.springboot.stream.properties.StreamJobProperties;
import com.leomaster.constants.CdsStreamConfigOptions;
import com.leomaster.core.constants.CohortConstants;
import com.leomaster.core.util.FileLoadUtils;
import com.leomaster.poly.PolyEventsWideTable;
import com.leomaster.related.RelatedEventsWideTable;
import com.leomaster.wash.WashLoginApp;
import com.leomaster.wash.WashRechargeApp;
import com.leomaster.wash.WashUserApp;
import com.leomaster.wash.WashWithdrawalApp;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * @author suyh
 * @since 2024-01-17
 */
public class CdsFlinkJobStreamApp {
    public static final String ACTIVE_KEY = CohortConstants.ACTIVE_KEY;

    public static void main(String[] arg) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        flinkStarter(env);
    }

    public static void flinkStarter(StreamExecutionEnvironment env) throws Exception {
        ReadableConfig readableConfig = env.getConfiguration();

        String yamlFilePath = readableConfig.get(CdsStreamConfigOptions.CDS_FLINK_SPRING_YAML_PATH);

        Map<String, Object> configProperties = new HashMap<>();
        FileLoadUtils.yamlConfigProperties(yamlFilePath, configProperties);

        Object activeValue = configProperties.get(ACTIVE_KEY);
        System.out.println("external configuration spring.profiles.active: " + activeValue);

        configProperties.put(ConfigConstants.JOB_TYPE_KEY, ConfigConstants.JOB_TYPE_STREAM);

        FlinkSpringContext.init(configProperties);

        StreamJobProperties properties = FlinkSpringContext.getBean(StreamJobProperties.class);
        // 重启策略时间调长一点，以处理rabbitmq 重启导致flink 结束的问题。
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 120000));

        Map<String, DataStream<String>> linkedDSMap = new HashMap<>();
        WashUserApp.setup(env, linkedDSMap, properties);
        WashLoginApp.setup(env, linkedDSMap, properties);
        WashRechargeApp.setup(env, linkedDSMap, properties);
        WashWithdrawalApp.setup(env, linkedDSMap, properties);

        RelatedEventsWideTable.setup(configProperties, linkedDSMap);

        PolyEventsWideTable.setup(configProperties, linkedDSMap);
        env.execute("poly-stats-job");

        FlinkSpringContext.closeContext();

        System.out.println("main finished.");
    }
}
