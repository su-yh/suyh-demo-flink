package com.suyh.cdc;

import com.mysql.jdbc.Driver;
import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.types.Row;

/**
 * @author suyh
 * @since 2024-03-12
 */
public class FlinkMysqlSourceMain {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
        String[] fieldNames = new String[]{"id", "uid"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat().setDrivername(Driver.class.getName())
                .setDBUrl("jdbc:mysql://192.168.8.34:3306/flink-cds")
                .setUsername("flink-cds")
                .setPassword("flink-cds")
                .setQuery("select id, uid from tb_user limit 1000")
                .setRowTypeInfo(rowTypeInfo)
                .setFetchSize(10) // 每次取1000 条
                // .setDialect(new MySqldialect)
                .finish();

        // 从MySQL读取数据
        DataSource<Row> rowDataSource = env.createInput(jdbcInputFormat);

        // 对数据进行处理，这里示例为简单的map操作
        MapOperator<Row, TbUserEntity> mapOperator = rowDataSource.map(new MapFunction<Row, TbUserEntity>() {
            @Override
            public TbUserEntity map(Row row) throws Exception {
                Long id = row.getFieldAs("id");
                String uid = row.getFieldAs("uid");
                TbUserEntity entity = new TbUserEntity();
                entity.setId(id).setUid(uid);
                System.out.println("row result, field1: " + row.getField(0) + ", field2: " + row.getField(1));
//                return Tuple2.of((Integer) row.getField(0), (String) row.getField(1));
                return entity;
            }
        });

        mapOperator.print();

        // 执行作业
        env.execute("Flink Batch MySQL Example");

    }

    @Data
    public static class TbUserEntity {
        private Long id;
        private String uid;
    }

}
