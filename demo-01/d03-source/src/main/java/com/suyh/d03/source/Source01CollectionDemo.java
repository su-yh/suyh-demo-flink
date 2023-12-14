package com.suyh.d03.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Source01CollectionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合读取数据
        DataStreamSource<Integer> source = env
                .fromElements(1,2,33); // 从元素读
//                .fromCollection(Arrays.asList(1, 22, 3));  // 从集合读


        source.print();

        env.execute();

    }
}
