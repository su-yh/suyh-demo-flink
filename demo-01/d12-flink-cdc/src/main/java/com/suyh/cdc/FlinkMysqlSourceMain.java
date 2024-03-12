package com.suyh.cdc;

import com.mysql.jdbc.Driver;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

/**
 * @author suyh
 * @since 2024-03-12
 */
public class FlinkMysqlSourceMain {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 设置MySQL连接信息
//        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl("jdbc:mysql://192.168.8.34:3306/test")
//                .withDriverName("com.mysql.cj.jdbc.Driver")
//                .withUsername("username")
//                .withPassword("password")
//                .build();

        TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
        String[] fieldNames = new String[]{"id", "uid"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat().setDrivername(Driver.class.getName())
                .setDBUrl("jdbc:mysql://192.168.8.34:3306/flink-cds")
                .setUsername("flink-cds")
                .setPassword("flink-cds")
                .setQuery("select id, uid from tb_user limit 10")
                .setRowTypeInfo(rowTypeInfo)
                // .setDialect(new MySqldialect)
                .finish();

        // 从MySQL读取数据
        DataSet<Row> inputDataSet = env.createInput(jdbcInputFormat);

        // 对数据进行处理，这里示例为简单的map操作
        DataSet<Tuple2<Integer, String>> resultDataSet = inputDataSet.map(new MapFunction<Row, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(Row row) throws Exception {
                System.out.println("row result, field1: " + row.getField(0) + ", field2: " + row.getField(1));
//                return Tuple2.of((Integer) row.getField(0), (String) row.getField(1));
                return Tuple2.of(1, "1");
            }
        });

        resultDataSet.print();

        // 执行作业
        env.execute("Flink Batch MySQL Example");

    }


    // 需要数据库开启binlog
    public static void main1(String[] args) throws Exception {
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
