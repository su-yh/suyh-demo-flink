package com.suyh.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author suyh
 * @since 2023-11-16
 */
public class WordCount03StreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // IDEA 运行时，也可以看到webui, 一般用于本地测试
        // 需要引入一个依赖: flink-runtime-web
        // 然后就可以在本地使用 http://localhost:8081/#/overview 进行访问 了。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(3);  // 全局指定并行度为：3

        DataStreamSource<String> socketDS = env.socketTextStream("www.suyh.com.cn", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap(
                (String value, Collector<Tuple2<String, Integer>> out) -> {
                    // value: 一行数据
                    String[] words = value.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);

                        // 调用采集器，使用Collector 向下游发送数据
                        out.collect(wordTuple2);
                    }
                }).setParallelism(2)    // 指定并行度为：2
                // The generic type parameters of 'Collector' are missing. In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved. An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface. Otherwise the type has to be specified explicitly using type information.
                .returns(Types.TUPLE(Types.STRING, Types.INT))  // 这一步的处理是因为lomda 表达式的类型插除问题，如果漏了，将会有异常报出。
                .keyBy(value -> value.f0).sum(1);

        sum.print();

        env.execute();
    }
}
