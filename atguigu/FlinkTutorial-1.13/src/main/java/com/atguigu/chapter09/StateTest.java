package com.atguigu.chapter09;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

public class StateTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.keyBy(data -> data.user)
                        .flatMap(new MyFlatMap())
                .name("taskName")  // 这个任务名称可以在WEB UI 界面上显示
                                .print();


        env.execute();
    }

    // 实现自定义的FlatMapFunction，用于Keyed State测试
    public static class MyFlatMap extends RichFlatMapFunction<Event, String>{
        // suyh - 所有的状态都按key 进行划分，不同的key 得到的状态是不一样的。它的底层维护了一个类似map 的结构，对每个key 进行区分。
        // 定义状态
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event, String> myAggregatingState;

        // 增加一个本地变量进行对比
        Long count = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("job id: " + getRuntimeContext().getJobId());
            // suyh - task name 都是一样的： Flat Map -> Sink: Print to Std. Out
            System.out.println("task name: " + getRuntimeContext().getTaskName());
            // suyh - (1/3) (2/3) (3/3)
            System.out.println("task name with subtasks: " + getRuntimeContext().getTaskNameWithSubtasks());
            // suyh - 该子任务的索引：0 1 2
            System.out.println("index of this subtasks: " + getRuntimeContext().getIndexOfThisSubtask());
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("my-state", Event.class);
            myValueState = getRuntimeContext().getState(valueStateDescriptor);

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));

            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reduce",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) throws Exception {
                            return new Event(value1.user, value1.url, value2.timestamp);
                        }
                    }
                    , Event.class));

            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-agg",
                    new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Event value, Long accmulator) {
                            return accmulator + 1;
                        }

                        @Override
                        public String getResult(Long accumulator) {
                            return "count: " + accumulator;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return a + b;
                        }
                    }
                    , Long.class));

            // 配置状态的TTL
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 访问和更新状态
            System.out.println(myValueState.value());
            myValueState.update(value);
            System.out.println( "my value: " + myValueState.value() );

            myListState.add(value);

            myMapState.put(value.user, myMapState.get(value.user) == null? 1: myMapState.get(value.user) + 1);
            System.out.println( "my map value: " + myMapState.get(value.user) );

            myReducingState.add(value);
            System.out.println( "my reducing value: " + myReducingState.get() );

            myAggregatingState.add(value);
            System.out.println( "my agg value: " + myAggregatingState.get() );

            count ++;
            System.out.println("count: " + count);
        }
    }
}
