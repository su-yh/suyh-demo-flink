package com.suyh.d01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream 实现 wordCount: 读文件（有界流）
 *
 * @author suyh
 * @since 2023-11-16
 */
public class WordCount02StreamDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据
        DataStreamSource<String> lineDS = env.readTextFile("demo-01/d01/input/word.txt");

        // 3. 处理数据：切换、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne
                = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // value: 一行数据
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);

                    // 调用采集器，使用Collector 向下游发送数据
                    out.collect(wordTuple2);
                }
            }
        });

        // 3.2 分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS
                = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            /**
             * 在这个方法里面提取出分组key
             */
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 3.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOneKS.sum(1);

        // 4. 输出数据
        sum.print();

        // 5. 执行：类似 spring streaming 最后 ssc.start();
        env.execute();
    }
}
