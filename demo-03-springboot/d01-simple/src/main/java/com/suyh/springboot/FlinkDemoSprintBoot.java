package com.suyh.springboot;

import com.suyh.springboot.boot.jobmgr.JobManagerSpringContext;
import com.suyh.springboot.boot.jobmgr.config.properties.FlinkSpringBootProperties;
import com.suyh.springboot.task.RichFlatMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.StringUtils;

import java.util.UUID;

/**
 * @author suyh
 * @since 2023-11-16
 */
@Slf4j
public class FlinkDemoSprintBoot {
    public static final ConfigOption<String> SUYH_CFG =
            ConfigOptions.key("state.savepoints.dir")
                    .stringType()
                    .defaultValue("suyh-default-value")
                    .withDeprecatedKeys("savepoints.state.backend.fs.dir")
                    .withDescription(
                            "The default directory for savepoints. Used by the state backends that write savepoints to"
                                    + " file systems (HashMapStateBackend, EmbeddedRocksDBStateBackend).");

    public static void main(String[] args) throws Exception {
        // 这里的参数处理，参数名与参数值之间是以空格分隔的，同时自定义的参数要放在最后面，这个我只是实验得出的结论，并非年源代码。
        // 使用示例：'--suyh.profiles.active suyh'
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String activeValue = parameterTool.get("suyh.profiles.active");
        int port = 8888;
        String suyhPort = parameterTool.get("suyh.port");
        if (StringUtils.hasText(suyhPort)) {
            port = Integer.parseInt(suyhPort);
        }

        log.info("suyh - main begin...");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // IDEA 运行时，也可以看到webui, 一般用于本地测试
        // 需要引入一个依赖: flink-runtime-web
        // 然后就可以在本地使用 http://localhost:8081 进行访问 了。
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ConfigurableApplicationContext context = JobManagerSpringContext.getContext(args);
        FlinkSpringBootProperties properties = context.getBean(FlinkSpringBootProperties.class);
        log.info("suyh.flink.parallelism: {}", properties.getParallelism());

        log.info("suyh - main begin...");
        if (properties.getParallelism() != null) {
            env.setParallelism(properties.getParallelism());
        }

        ReadableConfig configuration = env.getConfiguration();
        String suyhValue = configuration.get(SUYH_CFG);
        System.out.println("suyhValue: " + suyhValue);

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
                        return UUID.randomUUID().toString().replace("-", "");
                    }
                },
                1_000_000,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        DataStreamSource<String> stringDataStreamSource
                = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new RichFlatMap(args))
                .keyBy(value -> value.f0).sum(1);

        sum.print("last result");

        env.execute();
        JobManagerSpringContext.closeContext();
        log.info("suyh - main finished.");
    }
}
