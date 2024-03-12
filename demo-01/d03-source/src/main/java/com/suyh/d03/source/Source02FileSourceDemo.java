package com.suyh.cdc.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Source02FileSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 从文件读： 新Source架构

        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("demo-01/input/word.txt"),
                        new Path("demo-01/input/word.txt")
                )
                .build();

        env
                // 第三个参数是取的名字，目前来说并不重要，自己取就可以了。
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource")
                .print();


        env.execute();
    }
}
/**
 *
 * 新的Source写法：
 *   env.fromSource(Source的实现类，Watermark，名字)
 *
 */