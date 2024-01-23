package com.leomaster;

import com.aiteer.springboot.common.constants.ConfigConstants;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.batch.poly.BatchPolyEventsWideTable;
import com.leomaster.batch.related.BatchEventsWideTable;
import com.leomaster.batch.source.SourceLoader;
import com.leomaster.batch.wash.BatchWashLoginApp;
import com.leomaster.batch.wash.BatchWashRechargeApp;
import com.leomaster.batch.wash.BatchWashUserApp;
import com.leomaster.batch.wash.BatchWashWithdrawalApp;
import com.leomaster.constants.CdsBatchConfigOptions;
import com.leomaster.core.constants.CohortConstants;
import com.leomaster.core.util.FileLoadUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.springframework.util.StringUtils;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author suyh
 * @since 2024-01-22
 */
@Slf4j
public class CdsFlinkJobBatchApp {
    public static final String ACTIVE_KEY = CohortConstants.ACTIVE_KEY;

    public static void main(String[] args) throws Exception {
        int date = 20230508;
        String channel = "";


        if (args != null && args.length > 0) {
            try {
                String dataParam = args[0];
                if (dataParam.contains("date=")) {
                    date = Integer.parseInt(dataParam.substring(dataParam.indexOf('=') + 1));
                    log.info("date from param = " + date);
                }
            } catch (Exception e) {
                log.error("Exception:{}", e);
                e.printStackTrace();
            }

            if (args.length > 1) {
                try {
                    String channelParam = args[1];
                    if (channelParam.contains("channel=")) {
                        channel = channelParam.substring(channelParam.indexOf('=') + 1);
                        log.info("channel from param = " + channel);
                    }
                } catch (Exception e) {
                    log.error("Exception:{}", e);
                    e.printStackTrace();
                }
            }
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
        env.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
                log.info("onJobSubmitted, time = " + new Date());
            }

            @Override
            public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
                log.info("onJobExecuted, time = " + new Date());
            }
        });

        ReadableConfig readableConfig = env.getConfiguration();
        String yamlFilePath = readableConfig.get(CdsBatchConfigOptions.CDS_FLINK_SPRING_YAML_PATH);

        Map<String, Object> configProperties = new HashMap<>();
        FileLoadUtils.yamlConfigProperties(yamlFilePath, configProperties);

        configProperties.put(ConfigConstants.JOB_TYPE_KEY, ConfigConstants.JOB_TYPE_BATCH);
        Object activeEnv = configProperties.get(ACTIVE_KEY);
        if (activeEnv == null || !StringUtils.hasText(activeEnv.toString())) {
            activeEnv = "suyh";
            configProperties.put(ACTIVE_KEY, activeEnv);
        }
        System.out.println(ACTIVE_KEY + ": " + activeEnv);

        //load source from db
        Map<String, Collection<JSONObject>> sourceMap = SourceLoader.loadDbDataByDate(date, channel, configProperties);
        //data check

        Map<String, DataSet<String>> linkedDSMap = new HashMap<>();
        BatchWashUserApp.setup(env, sourceMap, linkedDSMap);
        BatchWashLoginApp.setup(env, sourceMap, linkedDSMap);
        BatchWashRechargeApp.setup(env, sourceMap, linkedDSMap);
        BatchWashWithdrawalApp.setup(env, sourceMap, linkedDSMap);

        BatchEventsWideTable.setup(linkedDSMap, configProperties);

        BatchPolyEventsWideTable.setup(date, linkedDSMap, configProperties);

        env.execute("batch-replay-stats-job");
        System.out.println("main finished.");
    }
}
