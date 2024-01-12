package com.suyh.d02.flink.starter;

import com.suyh.d02.flink.func.SimpleInitRichMap;
import com.suyh.d02.springboot.environment.FlinkSpringbootConfigProperties;
import com.suyh.d02.springboot.jobmgr.JobManagerSpringContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.UUID;

/**
 * @author suyh
 * @since 2024-01-12
 */
@Slf4j
public class FlinkSpringBootConfigDataDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ReadableConfig configuration = env.getConfiguration();

        // 初始化springboot 的外置配置
        FlinkSpringbootConfigProperties.getInstance().init(configuration);

        ConfigurableApplicationContext context = JobManagerSpringContext.getContext(new String[0]);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return UUID.randomUUID().toString().replace("-", "");
                    }
                },
                1_000_000,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        DataStreamSource<String> uuidSource
                = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        SingleOutputStreamOperator<String> mapSource = uuidSource.map(new SimpleInitRichMap<>());
        mapSource.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
