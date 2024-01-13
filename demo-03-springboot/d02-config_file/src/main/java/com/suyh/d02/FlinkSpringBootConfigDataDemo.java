package com.suyh.d02;

import com.suyh.d02.flink.constants.FlinkSpringbootConfigConstants;
import com.suyh.d02.flink.func.FlinkSpringBootInitFilter;
import com.suyh.d02.flink.vo.FlinkUserEntity;
import com.suyh.d02.springboot.environment.FlinkSpringbootConfigProperties;
import com.suyh.d02.springboot.jobmgr.JobManagerSpringContext;
import com.suyh.d02.springboot.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * @author suyh
 * @since 2024-01-12
 */
@Slf4j
public class FlinkSpringBootConfigDataDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ReadableConfig configuration = env.getConfiguration();

        // 初始化springboot 的外置配置
        String yamlPath = configuration.get(FlinkSpringbootConfigConstants.FLINK_SPRINGBOOT_CONFIG_FILE);
        Map<String, Object> configProperties = FlinkSpringbootConfigProperties.parse(yamlPath);

        JobManagerSpringContext.init(new String[0], configProperties);

        FlinkUserEntity flinkUser = new FlinkUserEntity();
        flinkUser.setId(1L).setAge(18).setEmail("su787910081@163.com").setCreateDate(new Date());
        String flinkUserJson = JsonUtils.serializable(flinkUser);
        log.info("suyh - [main] flinkUserJson: {}", flinkUserJson);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return UUID.randomUUID().toString().replace("-", "");
                    }
                },
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        DataStreamSource<String> uuidSource
                = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        SingleOutputStreamOperator<String> mapSource = uuidSource.filter(new FlinkSpringBootInitFilter<>(configProperties));
        mapSource.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
