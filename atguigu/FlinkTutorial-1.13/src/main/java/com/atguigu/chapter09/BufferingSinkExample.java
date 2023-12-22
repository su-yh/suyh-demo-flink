package com.atguigu.chapter09;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // suyh - 开启检查点，并指定间隔时间
        env.enableCheckpointing(10000L);
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());

//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(""));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // suyh - 状态一致性
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // suyh - 最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // suyh - 超时未完成保存，丢弃该保存
        checkpointConfig.setCheckpointTimeout(60000);
        // suyh - 最大并发检查点数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // suyh - 是否开启外部持久化检查
        checkpointConfig.enableExternalizedCheckpoints(
                // suyh - (RETAIN_ON_CANCELLATION)当作业被手动取消时，保留检查点，而不是删除
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // suyh - 允许不做分界线对齐，前提是 CheckpointingMode.EXACTLY_ONCE 而且并发检查点的数量是1
        checkpointConfig.enableUnalignedCheckpoints();
        // suyh - 允许检查点失败的次数，当达到该值后作业将会被kill 掉
        checkpointConfig.setTolerableCheckpointFailureNumber(0);


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("input");

        // 批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        private final int threshold;
        private transient ListState<Event> checkpointedState;
        private List<Event> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (Event element: bufferedElements) {
                    // 输出到外部系统，这里用控制台打印模拟
                    System.out.println(element);
                }
                System.out.println("==========输出完毕=========");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            // 把当前局部变量中的所有元素写入到检查点中
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>(
                    "buffered-elements",
                    Types.POJO(Event.class));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            // 如果是从故障中恢复，就将ListState中的所有元素添加到局部变量中
            if (context.isRestored()) {
                for (Event element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
