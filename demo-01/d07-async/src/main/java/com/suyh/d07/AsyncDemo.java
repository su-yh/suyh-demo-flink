package com.suyh.d07;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author suyh
 * @since 2024-01-01
 */
@Slf4j
public class AsyncDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(1);

        env.disableOperatorChaining();

        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(10_000);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                log.info("gen value: {}", value);
                System.out.println("gen value: " + value);
                return "Number: " + value;
            }
        }, 100000000, RateLimiterStrategy.perSecond(100000), Types.STRING);

//        dataGeneratorSource.

        DataStreamSource<String> dataStreamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        SingleOutputStreamOperator<String> mapSource = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                log.info("suyh - map, value: {}", value);
                System.out.println("suyh - map, value: " + value);
                return value;
            }
        });

        KeyedStream<String, Long> stringLongKeyedStream = mapSource.keyBy(new KeySelector<String, Long>() {
            @Override
            public Long getKey(String value) throws Exception {
                return 1L;
            }
        });

        SingleOutputStreamOperator<String> unorderedWait = AsyncDataStream.unorderedWait(stringLongKeyedStream, new RichAsyncFunction<String, String>() {

            private final Random random = new Random();
            private ExecutorService executorService;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                executorService = Executors.newFixedThreadPool(5);
            }

            @Override
            public void timeout(String value, ResultFuture<String> resultFuture) throws Exception {
//                super.timeout(value, resultFuture);
                executorService.submit(() -> {
                    try {
                        int waitMs = random.nextInt(5000) + 1000;
                        log.info("value: {}, wait ms: {}", value, waitMs);
                        System.out.println("value: " + value + ", wait ms: " + waitMs);
                        TimeUnit.MILLISECONDS.sleep(waitMs);

                        Collection<String> result = Collections.singletonList(value);
                        resultFuture.complete(result);
                    } catch (Exception exception) {
                        exception.printStackTrace();
                        resultFuture.completeExceptionally(exception);
                    }
                });
            }

            @Override
            public void asyncInvoke(String value, ResultFuture<String> resultFuture) throws Exception {
                log.info("asyncInvoke value: {}", value);
                System.out.println("asyncInvoke value: " + value);
                executorService.submit(() -> {
                    Collection<String> result = Collections.emptyList();
                    try {
                        int waitMs = random.nextInt(5000) + 10000;
                        log.info("value: {}, wait ms: {}", value, waitMs);
                        System.out.println("value: " + value + ", wait ms: " + waitMs);
                        TimeUnit.MILLISECONDS.sleep(waitMs);

                        result = Collections.singletonList(value);
                    } catch (Exception exception) {
                        exception.printStackTrace();
                        resultFuture.completeExceptionally(exception);
                    } finally {
                        resultFuture.complete(result);
                    }
                });
            }
        }, 19, TimeUnit.SECONDS, 100);

        KeyedStream<String, Long> keyedStream = unorderedWait.keyBy(new KeySelector<String, Long>() {
            @Override
            public Long getKey(String s) throws Exception {
                return 1L;
            }
        });

        SingleOutputStreamOperator<Long> countDs = keyedStream.process(new KeyedProcessFunction<Long, String, Long>() {
            private ValueState<Long> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            }

            @Override
            public void processElement(String value, KeyedProcessFunction<Long, String, Long>.Context ctx, Collector<Long> out) throws Exception {
                Long countValue = valueState.value();
                if (countValue == null) {
                    countValue = 0L;
                }
                countValue++;
                valueState.update(countValue);

                out.collect(countValue);
            }
        });

        countDs.print("suyh - result");

        env.execute();
    }
}
