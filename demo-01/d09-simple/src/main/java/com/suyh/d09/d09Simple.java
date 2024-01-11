package com.suyh.d09;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author suyh
 * @since 2024-01-08
 */
@Slf4j
public class d09Simple {
    public static final ConfigOption<String> SUYH_CFG =
            ConfigOptions.key("suyh.flink.key")
                    .stringType()
                    .defaultValue("suyh-default-value")
                    .withDeprecatedKeys("savepoints.state.backend.fs.dir")
                    .withDescription(
                            "test.");


    public static void main(String[] args) throws Exception {
        log.info("suyh - main begin...");
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String suyhActive = parameterTool.get("suyh.active");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // IDEA 运行时，也可以看到webui, 一般用于本地测试
        // 需要引入一个依赖: flink-runtime-web
        // 然后就可以在本地使用 http://localhost:8081 进行访问 了。
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        if ("local".equals(suyhActive)) {
            env.setParallelism(1);
        }

        ReadableConfig configuration = env.getConfiguration();
        String suyhValue = configuration.get(SUYH_CFG);
        System.out.println("suyh.flink.key: " + suyhValue);

        /*
         * 数据生成器Source，四个参数：
         *     第一个： GeneratorFunction接口，需要实现， 重写map方法， 输入类型固定是Long
         *     第二个： long类型， 自动生成的数字序列（从0自增）的最大值(小于)，达到这个值就停止了
         *     第三个： 限速策略， 比如 每秒生成几条数据
         *     第四个： 返回的类型
         */
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "number: " + value;
                    }
                },
                1000,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        DataStreamSource<String> stringDataStreamSource
                = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        stringDataStreamSource.print("last result");

        env.execute();
        log.info("suyh - main finished.");
    }
}
