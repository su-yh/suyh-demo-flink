package com.suyh;

import com.suyh.core.constants.SuyhConstants;
import com.suyh.core.constants.SuyhConfigOptions;
import com.suyh.core.util.FileLoadUtils;
import com.suyh.springboot.core.context.FlinkSpringContext;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author suyh
 * @since 2024-01-17
 */
public class FlinkJobSpringBooApp {
    public static final String ACTIVE_KEY = SuyhConstants.ACTIVE_KEY;

    public static void main(String[] arg) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        flinkStarter(env);
    }

    public static void flinkStarter(StreamExecutionEnvironment env) throws Exception {
        ReadableConfig readableConfig = env.getConfiguration();

        String yamlFilePath = readableConfig.get(SuyhConfigOptions.FLINK_SPRINGBOOT_CONFIG_PATH);

        Map<String, Object> configProperties = new HashMap<>();
        FileLoadUtils.yamlConfigProperties(yamlFilePath, configProperties);

        Object activeValue = configProperties.get(ACTIVE_KEY);
        System.out.println("external configuration spring.profiles.active: " + activeValue);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return UUID.randomUUID().toString().replace("-", "");
                    }
                },
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        DataStreamSource<String> uuidSource
                = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        SingleOutputStreamOperator<String> mapSource = uuidSource.filter(new RichFilterFunction<String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // 在所有需要用到spring 相关功能的地方进行初始化
                FlinkSpringContext.init(configProperties);
            }

            @Override
            public void close() throws Exception {
                super.close();

                // 在所有需要用到spring 相关功能的地方进行资源释放
                FlinkSpringContext.closeContext();
            }

            @Override
            public boolean filter(String value) throws Exception {
                return true;
            }
        });
        mapSource.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("main finished.");
    }
}
