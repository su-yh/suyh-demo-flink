package com.suyh.cdc.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
@Slf4j
public class Source04DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 如果有n个并行度， 最大值设为a
        // 将数值 均分成 n份，  a/n ,比如，最大100，并行度2，每个并行度生成50个
        // 其中一个是 0-49，另一个50-99
        env.setParallelism(16);

        DataStreamSource<TempVo> source = env
                .fromElements(
                        new TempVo(1L, "name-1"), new TempVo(1L, "name-2"), new TempVo(1L, "name-3"),
                        new TempVo(2L, "name-1"), new TempVo(2L, "name-2"), new TempVo(2L, "name-3"),
                        new TempVo(3L, "name-1"), new TempVo(3L, "name-2"), new TempVo(3L, "name-3")
                );


        source.keyBy(new KeySelector<TempVo, Long>() {
                    @Override
                    public Long getKey(TempVo vo) throws Exception {
                        return vo.getUid();
                    }
                })
//                .process(new KeyedProcessFunction<Long, TempVo, TempVo>() {
//                    private static final long serialVersionUID = -2976464563244160702L;
//
//                    @Override
//                    public void processElement(TempVo value, KeyedProcessFunction<Long, TempVo, TempVo>.Context ctx, Collector<TempVo> out) throws Exception {
//                        log.info("current key: {}, uid: {}, name: {}", ctx.getCurrentKey(), value.getUid(), value.getName());
//                        out.collect(value);
//                    }
//                })
//                .map(new MapFunction<TempVo, TempVo>() {
//                    @Override
//                    public TempVo map(TempVo value) throws Exception {
//                        log.info("value, id: {}, name: {}", value.getUid(), value.getName());
//                        String name = value.getName();
//                        value.setName(name + "-after");
//                        return value;
//                    }
//                })
                .keyBy(new KeySelector<TempVo, String>() {
                    @Override
                    public String getKey(TempVo value) throws Exception {
                        return value.getName();
                    }
                })
                .process(new KeyedProcessFunction<String, TempVo, ResultVo>() {
                    private static final long serialVersionUID = 1061250054257712457L;

                    private ValueState<ResultVo> valueNameState;
                    private int indexOfThisSubtask;
                    private int numberOfParallelSubtasks;

                    //       // 定义状态变量，用来保存已经到达的事件
                    //        private ValueState<Tuple3<String, String, Long>> appEventState;
                    //        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;
                    //
                    //        @Override
                    //        public void open(Configuration parameters) throws Exception {
                    //            appEventState = getRuntimeContext().getState(
                    //                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                    //            );
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        valueNameState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("suyh", ResultVo.class));
                        indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
                    }

                    @Override
                    public void processElement(TempVo value, KeyedProcessFunction<String, TempVo, ResultVo>.Context ctx, Collector<ResultVo> out) throws Exception {
                        log.info("seconds, current key: {}, uid: {}, name: {}. indexOfThisSubtask: {}, numberOfParallelSubtasks: {}",
                                ctx.getCurrentKey(), value.getUid(), value.getName(), indexOfThisSubtask, numberOfParallelSubtasks);
                        ResultVo vo = valueNameState.value();
                        if (vo == null) {
                            vo = new ResultVo();
                            vo.setKey(ctx.getCurrentKey());
                            vo.setListVo(new ArrayList<>());
                        }

                        List<TempVo> listVo = vo.getListVo();
                        listVo.add(value);

                        valueNameState.update(vo);
                        out.collect(vo);
                    }
                })
                .map(new MapFunction<ResultVo, String>() {
                    @Override
                    public String map(ResultVo value) throws Exception {
                        log.info("map result: {}", value);
                        return value.toString();
                    }
                })
                .print();


        env.execute();
    }

    @AllArgsConstructor
    @Data
    public static class TempVo {
        private Long uid;
        private String name;
    }

    @Data
    public static class ResultVo {
        private String key;
        private List<TempVo> listVo;
    }
}
