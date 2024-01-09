package com.suyh.d02.rmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * @author suyh
 * @since 2024-01-09
 */
@Slf4j
public class RmqF13Demo {
    private final static RMQConnectionConfig CONNECTION_CONFIG = new RMQConnectionConfig.Builder()
            .setHost("139.9.50.208")
            .setPort(5672)
            .setUserName("admin")
            .setPassword("adminadmin")
            .setVirtualHost("/flinkhost")
            .setPrefetchCount(100)
            .setNetworkRecoveryInterval(30_000)
            .setConnectionTimeout(30_000)
            .setAutomaticRecovery(true)
            .build();

    private final static String POLY_TB_RECHARGE = "poly_tb_recharge_pre";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(2);

        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(6_000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        RMQSource<String> rmqSource = new RMQSource<>(CONNECTION_CONFIG, POLY_TB_RECHARGE, true, new SimpleStringSchema());

        DataStreamSource<String> dataStreamSource = env.addSource(rmqSource, "testSource");
        dataStreamSource.print();

        log.info("suyh - execute");
        env.execute();
    }
}
