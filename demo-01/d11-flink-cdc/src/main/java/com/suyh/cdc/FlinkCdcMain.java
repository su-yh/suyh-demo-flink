package com.suyh.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author suyh
 * @since 2024-03-12
 */
public class FlinkCdcMain {
    // 需要数据库开启binlog
    public static void main(String[] args) throws Exception {
        // 1. 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 通过flink cdc 构建 source function
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("192.168.8.34")
                .port(3306)
                .username("flink-cds")
                .password("flink-cds")
                .databaseList("flink-cds")  // 可以多个库
                .tableList("flink-cds.tb_user")   // 可以多个表，需要带上库名
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();


        DataStreamSource<String> dataStreamSource =env.addSource(sourceFunction);

        dataStreamSource.print();

        env.execute("flink-cdc");

    }
}
