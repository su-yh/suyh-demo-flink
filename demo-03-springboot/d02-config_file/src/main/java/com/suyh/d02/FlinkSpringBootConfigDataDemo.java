package com.suyh.d02;

import com.suyh.d02.flink.func.FlinkSpringBootInitFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author suyh
 * @since 2024-01-12
 */
@SpringBootApplication
@Slf4j
public class FlinkSpringBootConfigDataDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 得到flink-conf.yaml 文件中的所有配置，但有一点要注意，该文件的解析只能一行一行的，不能像spring-boot 一样多行。
        // 如：下面的方式是不允许的，只能写在一行。spring.profiles.active: prod
        // spring:
        //  profiles:
        //    active: prod
        ReadableConfig readableConfig = env.getConfiguration();
        Configuration configuration = (Configuration) readableConfig;
        Map<String, String> flinkConfYamlMap = configuration.toMap();
        System.out.println("flink-conf.yaml configuration size: " + flinkConfYamlMap.size());
        flinkConfYamlMap.forEach((k, v) -> System.out.println("suyh - configuration, " + k + ": " + v));

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
        SingleOutputStreamOperator<String> mapSource = uuidSource.filter(new FlinkSpringBootInitFilter<>(new HashMap<>(flinkConfYamlMap)));
        mapSource.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
