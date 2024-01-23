package com.leomaster.batch.wash;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class BatchWashLoginApp {

    private final static Logger LOGGER = LoggerFactory.getLogger(BatchWashLoginApp.class);


    public static void setup(ExecutionEnvironment env, Map<String, Collection<JSONObject>> sourceMap,
                             Map<String, DataSet<String>> linkedDSMap) {
        String sourceQueue = "replay_tb_user_login";
        String sinkQueue = "dwd_tb_user_login";

        Collection<JSONObject> source = sourceMap.get(sourceQueue);
        if (CollectionUtil.isNullOrEmpty(source)) {
            LOGGER.info("BatchWashLoginApp replay_tb_user_login is null");
            return;
        }
        DataSet<JSONObject> jsonStrDS = env.fromCollection(source);

        MapOperator<JSONObject, String> rmqDS = jsonStrDS.distinct((KeySelector<JSONObject, String>) jsonObj -> jsonObj.getString("uid") + "_" + jsonObj.getString("channel")).map(jsonObj -> jsonObj.toJSONString());

        linkedDSMap.put(sinkQueue, rmqDS);
    }
}
