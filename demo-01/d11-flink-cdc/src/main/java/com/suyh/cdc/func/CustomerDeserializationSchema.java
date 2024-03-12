package com.suyh.cdc.func;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author suyh
 * @since 2024-03-12
 */
public class CustomerDeserializationSchema implements DebeziumDeserializationSchema {
    private static final long serialVersionUID = 484814052839322765L;

    /**
     * {
     *     "db": "",
     *     "tableName": "",
     *     "before": "{详细记录}",
     *     "after": "{详细记录}",
     *     "op": ""
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        JSONObject result = new JSONObject();

        // 库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db", fields[1]);
        result.put("tableName", fields[2]);

        // before 数据
        Struct value = (Struct)sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
//        Schema schema = value.schema();

        if (before != null) {
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);

        // after 数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
//        Schema schema = value.schema();

        if (after != null) {
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                afterJson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterJson);

        // 操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);

        collector.collect(result.toString());
    }

    @Override
    public TypeInformation getProducedType() {
        return null;
    }
}
