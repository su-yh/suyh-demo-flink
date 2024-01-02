package com.suyh.d08.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author suyh
 * @since 2024-01-02
 */
@Slf4j
public class StringSchema extends SimpleStringSchema {
    @Override
    public String deserialize(byte[] message) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String curTime = sdf.format(new Date());
        System.out.println("curTime: " + curTime + ", suyh - deserialize 2222");
        try {
            TimeUnit.SECONDS.sleep(1L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return super.deserialize(message);
    }
}
