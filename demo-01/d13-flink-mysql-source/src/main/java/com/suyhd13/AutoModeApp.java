package com.suyhd13;

import com.suyhd13.vo.TbUserDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 使用流批一体API 实现流批一套作业的实现。
 *
 * @author suyh
 * @since 2024-03-21
 */
@Slf4j
public class AutoModeApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        env.disableOperatorChaining();

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        startBatch(env);


        env.execute("poly-stats-job");

        System.out.println("main finished.");
    }

    public static void startBatch(StreamExecutionEnvironment env) {
        String tableFields = "id,uid,channel,ctime,gaid,origin_channel,vungo_user_id,day,cts,pn";
        String[] fieldNames = tableFields.split(",");

        String querySql = "select " + tableFields + " from tb_user limit 100";

        TypeInformation<?>[] fieldTypes = new TypeInformation[]{
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://192.168.8.34:3306/flink-cds?useSSL=false&characterEncoding=UTF-8")
                .setUsername("flink-cds")
                .setPassword("flink-cds")
                .setQuery(querySql)
                .setRowTypeInfo(rowTypeInfo)
                .setFetchSize(1000) // 每次取1000 条
                // .setDialect(new MySqldialect)
                .finish();

//        env.fromSource()

//        DataStream<Row> rowDataStreamSource = env.createInput(jdbcInputFormat, TypeExtractor.getInputFormatTypes(jdbcInputFormat));
        DataStream<Row> rowDataStreamSource = dataStreamBatch(env, jdbcInputFormat);
        if (rowDataStreamSource == null) {
            return;
        }
        SingleOutputStreamOperator<TbUserDto> dtoDataStream
                = rowDataStreamSource.map(AutoModeApp::rowToEntity)
                .name("userReg001_mapDto");

        KeyedStream<TbUserDto, String> keyedStream = dtoDataStream.keyBy(new KeySelector<TbUserDto, String>() {
            @Override
            public String getKey(TbUserDto value) throws Exception {
                return value.getChannel();
            }
        });

        SingleOutputStreamOperator<TbUserDto> regDateStream = keyedStream.sum("day");
        regDateStream.print("suyh regDate");
    }

    private static TbUserDto rowToEntity(Row row) {
        TbUserDto dto = new TbUserDto();
        dto.setUid(row.getFieldAs("uid")).setChannel(row.getFieldAs("channel"))
                .setCtime(row.getFieldAs("ctime")).setGaid(row.getFieldAs("gaid"))
                .setDay(row.getFieldAs("day")).setPn(row.getFieldAs("pn"));

        return dto;
    }

    public static DataStream<Row> dataStreamBatch(StreamExecutionEnvironment env, JdbcInputFormat jdbcInputFormat) {
        try {
            String methodName = "addSource";
            TypeInformation<Row> typeInfo = TypeExtractor.getInputFormatTypes(jdbcInputFormat);
            InputFormatSourceFunction<Row> function =
                    new InputFormatSourceFunction<>(jdbcInputFormat, TypeExtractor.getInputFormatTypes(jdbcInputFormat));
            env.addSource(function, "CustomSourceName", typeInfo);
            Method addSourceMethod = ReflectionUtils.findMethod(StreamExecutionEnvironment.class, methodName, SourceFunction.class, String.class, TypeInformation.class, Boundedness.class);
            if (addSourceMethod == null) {
                log.error("method cannot found, method name: {}", methodName);
                return null;
            }
            ReflectionUtils.makeAccessible(addSourceMethod);

            Object object = addSourceMethod.invoke(env, function, "CustomSourceName", typeInfo, Boundedness.BOUNDED);
            if (!DataStream.class.isAssignableFrom(object.getClass())) {
                log.error("class not match.");
                return null;
            }
            return (DataStream<Row>) object;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }

    }
}
