package com.suyh.d01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * DataSet API 实现word count（不推荐）
 *
 * @author suyh
 * @since 2023-11-16
 */
public class WordCount01BatchDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        // 它会自己识别是本地环境还是远程环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据：从文件中读取
        DataSource<String> lineDS = env.readTextFile("demo-01/input/word.txt");

        // 3. 按行切分、转换 (word, 1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne
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


        // 4. 按单词分组
        // 按索引位置分组，word 在Tuple 中的位置 是第一个所以 这里传入: 0
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupBy = wordAndOne.groupBy(0);

        // 5. 按分组内聚合
        // 因为Integer 的位置是第二个，所以这里传入的值是: 1
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupBy.sum(1);

        // 6. 输出
        sum.print();
    }
}
