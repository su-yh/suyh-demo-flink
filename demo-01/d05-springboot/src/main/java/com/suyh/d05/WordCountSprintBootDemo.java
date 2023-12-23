package com.suyh.d05;

import com.suyh.d05.task.RichFlatMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author suyh
 * @since 2023-11-16
 */
@Slf4j
public class WordCountSprintBootDemo {
    public static void main(String[] args) throws Exception {
        log.info("suyh - main begin...");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // IDEA 运行时，也可以看到webui, 一般用于本地测试
        // 需要引入一个依赖: flink-runtime-web
        // 然后就可以在本地使用 http://localhost:8081 进行访问 了。
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        log.info("suyh - main begin...");
        // env.setParallelism(1);

        // 利用netcat 监听7777 端口： nc -lk 7777
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop001", 8888);

//        KeyedStream<String, String> keyedStream = socketDS.keyBy(value -> value);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap(new RichFlatMap(args))
                .keyBy(value -> value.f0).sum(1);

        sum.print("last result");

        env.execute();
        log.info("suyh - main finished.");
    }
}
