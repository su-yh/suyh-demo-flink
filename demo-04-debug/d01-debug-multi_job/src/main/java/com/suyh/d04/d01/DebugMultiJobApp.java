package com.suyh.d04.d01;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author suyh
 * @since 2024-01-25
 */
public class DebugMultiJobApp {
    public static void main(String[] arg) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        long count = 100;
        DataGeneratorSource<Long> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, Long>() {
                    @Override
                    public Long map(Long value) throws Exception {
                        return value;
                    }
                },
                count,
                RateLimiterStrategy.perSecond(1),
                Types.LONG
        );

        DataStreamSource<Long> uuidSource
                = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        SingleOutputStreamOperator<Long> filterSource = uuidSource.filter(new RichFilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return true;
            }
        });

        KeyedStream<Long, Long> keyedStream = filterSource.keyBy(value -> value < 50 ? 0L : 1L);

        SingleOutputStreamOperator<String> keyedSum = keyedStream.process(new KeyedProcessFunction<Long, Long, String>() {
            private static final long serialVersionUID = 8334146449056209282L;

            private ValueState<Long> sumState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("sumState", Types.LONG);
                sumState = getRuntimeContext().getState(stateDescriptor);


            }

            @Override
            public void processElement(Long value, KeyedProcessFunction<Long, Long, String>.Context ctx, Collector<String> out) throws Exception {
                Long currentKey = ctx.getCurrentKey();

                Long historySum = sumState.value();
                if (historySum == null) {
                    historySum = 0L;
                }

                Long sumValue = value + historySum;
                sumState.update(sumValue);

                String outResult = "key: " + currentKey + ", sum: " + sumValue;

                out.collect(outResult);
            }
        });

        keyedSum.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("main finished.");
    }
}
