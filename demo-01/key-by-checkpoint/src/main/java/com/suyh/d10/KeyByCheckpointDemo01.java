package com.suyh.d10;

import com.suyh.d10.vo.WaterSensor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 每一批次生成不同的数据量，观察不同数据量之间checkpoint 的存储大小
 *
 * @author suyh
 * @since 2024-01-10
 */
@Slf4j
public class KeyByCheckpointDemo01 {
    public static void main(String[] args) throws Exception {
        log.info("suyh - main begin...");
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String suyhActive = parameterTool.get("suyh.active");
        suyhActive = "local";
        // IDEA 运行时，也可以看到webui, 一般用于本地测试
        // 需要引入一个依赖: flink-runtime-web
        // 然后就可以在本地使用 http://localhost:8081 进行访问 了。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        if ("local".equals(suyhActive)) {
            env.setParallelism(1);
            env.disableOperatorChaining();
        }

        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("file:///opt/suyh/checkpoints");
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        // TODO: suyh - 每次修改该值
        // 每批次checkpoint 生成的数据量
        int recordsPerCheckpoint = 10;
        /*
         * 数据生成器Source，四个参数：
         *     第一个： GeneratorFunction接口，需要实现， 重写map方法， 输入类型固定是Long
         *     第二个： long类型， 自动生成的数字序列（从0自增）的最大值(小于)，达到这个值就停止了
         *     第三个： 限速策略， 比如 每秒生成几条数据
         *     第四个： 返回的类型
         */
        DataGeneratorSource<WaterSensor> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, WaterSensor>() {
                    @Override
                    public WaterSensor map(Long value) throws Exception {
                        long id = value % recordsPerCheckpoint + 1;
                        WaterSensor waterSensor = new WaterSensor();
                        waterSensor.setId("s" + id);
                        waterSensor.setTs(value);
                        waterSensor.setVc(value);
                        return waterSensor;
                    }
                },
                // 12 批checkpoint，每批recordsPerCheckpoint 数量
                12 * recordsPerCheckpoint,
                RateLimiterStrategy.perCheckpoint(recordsPerCheckpoint),
                Types.GENERIC(WaterSensor.class)
        );

        DataStreamSource<WaterSensor> dataStreamSource
                = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        // ####################
        dataStreamSource.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            ValueState<Long> lastVcState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                // TODO 1.创建 StateTtlConfig
                                StateTtlConfig stateTtlConfig = StateTtlConfig
                                        .newBuilder(Time.seconds(5)) // 过期时间5s
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态 创建和写入（更新） 更新 过期时间
//                                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 状态 读取、创建和写入（更新） 更新 过期时间
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期的状态值
                                        .build();

                                // TODO 2.状态描述器 启用 TTL
                                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.LONG);
                                stateDescriptor.enableTimeToLive(stateTtlConfig);


                                this.lastVcState = getRuntimeContext().getState(stateDescriptor);

                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 先获取状态值，打印 ==》 读取状态
                                Long lastVc = lastVcState.value();
                                out.collect("key=" + value.getId() + ",状态值=" + lastVc);

//                                // 如果水位大于10，更新状态值 ===》 写入状态
//                                if (value.getVc() > 10) {
//                                    String currentKey = ctx.getCurrentKey();
//                                    System.out.println("current key: " + currentKey + ", update state, vc: " + value.getVc());
//                                    lastVcState.update(value.getVc());
//                                }

                                lastVcState.update(value.getVc());
                            }
                        }
                )
                .print();


        env.execute();
        log.info("suyh - main finished.");
    }
}
