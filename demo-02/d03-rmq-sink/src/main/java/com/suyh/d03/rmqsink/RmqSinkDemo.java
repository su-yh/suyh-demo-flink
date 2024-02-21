package com.suyh.d03.rmqsink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * @author suyh
 * @since 2024-02-21
 */
@Slf4j
public class RmqSinkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long count = 50;
        DataGeneratorSource<Long> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, Long>() {
                    @Override
                    public Long map(Long value) throws Exception {
                        return value;
                    }
                },
                count,
                RateLimiterStrategy.perSecond(10),
                Types.LONG
        );

        DataStreamSource<Long> uuidSource
                = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        SingleOutputStreamOperator<Long> filterSource = uuidSource.filter(new RichFilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                log.info("filer, value: {}", value);
                return true;
            }
        });

        SingleOutputStreamOperator<String> mapDataStream = filterSource.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                log.info("map, value: {}", value);
                return value + "";
            }
        });

        mapDataStream.addSink(buildRmqSink());

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("main finished.");
    }

    private static SinkFunction<String> buildRmqSink() {

        RMQConnectionConfig.Builder builder = new RMQConnectionConfig.Builder();
        builder.setHost("192.168.8.34").setPort(5672).setUserName("admin").setPassword("aiteer").setVirtualHost("/suyh-flinkhost");
        RMQConnectionConfig rmqConnectionConfig = builder.build();
        SimpleStringSchema schema = new SimpleStringSchema();
        String exchange = "flink_output_topic_exchange";
        // String routingKey = "cohort_key";
        String routingKey = "unknown_routing_key";
        ExchangeTopicPublicOptions<String> publicOptions = new ExchangeTopicPublicOptions<>(exchange, routingKey);

        return new ExchangeTopicRmqSink<>(rmqConnectionConfig, schema, publicOptions);
    }
}
