package com.suyh.demo;

import com.leomaster.CdsFlinkJobStreamApp;
import com.leomaster.constants.CdsStreamConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author suyh
 * @since 2024-01-22
 */
public class SuyhFlinkLocalStreamApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CdsStreamConfigOptions.CDS_FLINK_SPRING_YAML_PATH, "application-suyh_job.yaml");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(2);
        env.disableOperatorChaining();

        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointStorage("file:///opt/trend_oper/checkpoints");
        config.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setMinPauseBetweenCheckpoints(6_000L);
        // 设置超过检查点可容忍失败阈值
        config.setTolerableCheckpointFailureNumber(2);
        // chickpoint 超时时间
        config.setCheckpointTimeout(30 * 60 * 1000);

        CdsFlinkJobStreamApp.flinkStarter(env);
    }
}
