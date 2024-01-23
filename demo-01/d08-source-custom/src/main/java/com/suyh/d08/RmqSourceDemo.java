package com.suyh.d08;

import com.suyh.d08.schema.StringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * @author suyh
 * @since 2024-01-02
 */
public class RmqSourceDemo {
    public static void main(String[] args) throws Exception {
        // 这里的参数处理，参数名与参数值之间是以空格分隔的，同时自定义的参数要放在最后面，这个我只是实验得出的结论，并非年源代码。
        // 使用示例：'--suyh.profiles.active suyh'
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String activeValue = parameterTool.get("suyh.profiles.active");
        System.out.println("activeValue: " + activeValue);
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(2);
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(2000);

        RMQConnectionConfig rmqConfig = new RMQConnectionConfig.Builder()
                .setHost("isuyh.com")
                .setPort(5672)
                .setUserName("admin")
                .setPassword("adminadmin")
                .setVirtualHost("/flinkhost")
                // TODO: suyh - 指定每批次从mq 中取多少条数据来处理，每批次为一个checkpoint 间隔时间。
                //  只有在checkpoint 完成之后才会到MQ 中去取下一批次的数据。这样就可以做到限流的效果。
                .setPrefetchCount(100) // 这个还是尽可能的控制数量，该预取数量跟并行度没有关系。
                .build();
        RMQSource<String> rmqSource = new RMQSource<>(rmqConfig, "poly_tb_user_pre", true, new StringSchema());

        env.addSource(rmqSource, "rmqSource")
                .print("suyh - sink");

        env.execute();
    }
}
