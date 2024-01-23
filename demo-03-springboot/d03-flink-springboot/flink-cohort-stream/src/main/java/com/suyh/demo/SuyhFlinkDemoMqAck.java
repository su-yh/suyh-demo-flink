package com.suyh.demo;

import com.aiteer.springboot.common.vo.RmqConnectProperties;
import com.aiteer.springboot.core.context.FlinkSpringContext;
import com.aiteer.springboot.stream.properties.StreamJobProperties;
import com.leomaster.constants.CdsStreamConfigOptions;
import com.leomaster.core.util.FileLoadUtils;
import com.leomaster.utils.MyRMQUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;

import java.util.HashMap;
import java.util.Map;

/**
 * @author suyh
 * @since 2023-12-28
 */
public class SuyhFlinkDemoMqAck {
    public static void main(String[] arg) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set(CdsStreamConfigOptions.CDS_FLINK_SPRING_YAML_PATH, "D:\\work\\aiteer-flink\\flink-cohort-job\\application-cds_job.yaml");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        ReadableConfig readableConfig = env.getConfiguration();
        String yamlFilePath = readableConfig.get(CdsStreamConfigOptions.CDS_FLINK_SPRING_YAML_PATH);
        Map<String, Object> configProperties = new HashMap<>();
        FileLoadUtils.yamlConfigProperties(yamlFilePath, configProperties);
        {
            FlinkSpringContext.init(configProperties);
            StreamJobProperties properties = FlinkSpringContext.getBean(StreamJobProperties.class);
            StreamJobProperties.CdsRmq cdsRmq = properties.getCdsRmq();
            RmqConnectProperties sourceProp = cdsRmq.getSource().getConnect();
            // suyh - 将MQ 里面的所有数据清空。
            if (true) {
                {
                    RMQSource<String> rmqSource = MyRMQUtil.buildRmqSource(sourceProp, cdsRmq.getSource().getQueueUserRegistry(), false, null);
                    DataStream<String> jsonStrDS = env.addSource(rmqSource);
                    jsonStrDS.filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String value) throws Exception {
                            return false;
                        }
                    }).print();
                }

                {
                    RMQSource<String> rmqSource = MyRMQUtil.buildRmqSource(sourceProp, cdsRmq.getSource().getQueueUserLogin(), false, null);
                    DataStream<String> jsonStrDS = env.addSource(rmqSource);
                    jsonStrDS.filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String value) throws Exception {
                            return false;
                        }
                    }).print();
                }

                {
                    RMQSource<String> rmqSource = MyRMQUtil.buildRmqSource(sourceProp, cdsRmq.getSource().getQueueUserRecharge(), false, null);
                    DataStream<String> jsonStrDS = env.addSource(rmqSource);
                    jsonStrDS.filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String value) throws Exception {
                            return false;
                        }
                    }).print();
                }

                {
                    RMQSource<String> rmqSource = MyRMQUtil.buildRmqSource(sourceProp, cdsRmq.getSource().getQueueUserWithdrawal(), false, null);
                    DataStream<String> jsonStrDS = env.addSource(rmqSource);
                    jsonStrDS.filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String value) throws Exception {
                            return false;
                        }
                    }).print();
                }
            }
        }

        env.execute("poly-stats-job");

        FlinkSpringContext.closeContext();
    }

}
