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
 * 每一批checkpoint 生成固定数量的数据，并且每批次的key 都增加1 ，同时保留上一批次的key。
 * 即：第一批次的key: s1，第二批次的key: s1, s2 第三批次的key: s1, s2, s3，第n 批次的key: s1, s2, s3, ... sn
 * 当批次达到5 之后，key 的数量开始降低。
 * 即：第一批次：s1， 第二次批：s1, s2 第三批次: s1, s2, s3 第四批次： s1, s2, s3, s4 第五批次: s1, s2, s3, s4, s5
 * 第六批次: s1, s2, s3, s4 第七批次: s1, s2, s3 第八批次；s1, s2
 * 所以这种情况最多九批次。之后的批次的key 会变成s-1, s-2 不好看。哈哈
 *
 *
 * @author suyh
 * @since 2024-01-10
 */
@Slf4j
public class KeyByCheckpointDemo03 {
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

        int checkpointSecond = 5;   // checkpoint 间隔时间
        int stateTtlSecond = 3; // state 的ttl 时间
        env.enableCheckpointing(checkpointSecond * 1000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("file:///opt/suyh/checkpoints");
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

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
                        long idCount = value / recordsPerCheckpoint + 1;

                        int idModeFirst = 1;
                        int idModeLast = 5;

                        // 需要取一个对最终值取模的基础数值，  x % idMode = ...
                        int idMode = idModeFirst;
                        for (int i = 0; i < idCount; i++) {
                            // 如果没有判断，将会出现的现象是： 1 2 3 4 5 4 3 2 1 0
                            // 显然我期望的最大的只有5 最小的也只应该有1 ，这个0 是异常情况。
                            // 所以需要忽略每一个首次的自增与自减
                            // 以期望达到效果：1 2 3 4 5   5 4 3 2 1   1 2 3 4 5   5 4 3 2 1 ...
                            if (i % idModeLast != 0) {
                                int fileCount = i / idModeLast;
                                // 步长为1 则自增，步长为-1 则自减
                                int stride = (fileCount % 2) == 0 ? 1 : -1;
                                idMode += stride;
                            }

//                            System.out.println("idMode: " + idMode);
                        }
                        assert idMode > 0;
                        long idValue = value % idMode + 1;
//                        System.out.println("idCount: " + idCount + ", idValue: " + idValue);
                        WaterSensor waterSensor = new WaterSensor();
                        waterSensor.setId("s" + idValue);
                        waterSensor.setTs(value);
                        waterSensor.setVc(value);
                        return waterSensor;
                    }
                },
                // 12 批checkpoint，每批recordsPerCheckpoint 数量
                120 * recordsPerCheckpoint,
                RateLimiterStrategy.perCheckpoint(recordsPerCheckpoint),
                Types.GENERIC(WaterSensor.class)
        );

        DataStreamSource<WaterSensor> dataStreamSource
                = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        dataStreamSource.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            ValueState<Long> lastVcState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                // 1.创建 StateTtlConfig
                                StateTtlConfig stateTtlConfig = StateTtlConfig
                                        .newBuilder(Time.seconds(stateTtlSecond)) // 过期时间
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态 创建和写入（更新） 更新 过期时间
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期的状态值
                                        .build();

                                // 2.状态描述器 启用 TTL
                                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.LONG);
                                stateDescriptor.enableTimeToLive(stateTtlConfig);

                                this.lastVcState = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 先获取状态值，打印 ==》 读取状态
                                String currentKey = ctx.getCurrentKey();
                                out.collect("key: " + currentKey + ", vc 值=" + value.getVc());

                                lastVcState.update(value.getVc());
                            }
                        }
                )
                .print();


        env.execute();
        log.info("suyh - main finished.");
    }
}
