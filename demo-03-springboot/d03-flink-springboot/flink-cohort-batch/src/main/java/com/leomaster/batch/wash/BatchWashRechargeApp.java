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

public class BatchWashRechargeApp {

    private final static Logger LOGGER = LoggerFactory.getLogger(BatchWashRechargeApp.class);


    public static void setup(ExecutionEnvironment env, Map<String, Collection<JSONObject>> sourceMap,
                             Map<String, DataSet<String>> linkedDSMap) {
        String sourceQueue = "replay_tb_recharge";
        String sinkQueue = "dwd_tb_recharge";

        Collection<JSONObject> source = sourceMap.get(sourceQueue);
        if(CollectionUtil.isNullOrEmpty(source)){
            LOGGER.info("BatchWashRechargeApp replay_tb_recharge is null");
            return;
        }
        DataSet<JSONObject> jsonStrDS = env.fromCollection(source);

        //先filter
        MapOperator<JSONObject, String> rmqDS = jsonStrDS.distinct((KeySelector<JSONObject, String>) jsonObject -> jsonObject.getString("order")).map(jsonObj -> jsonObj.toJSONString());

        //尝试简化job的结构，直接存到map中，交给下一层的任务
        linkedDSMap.put(sinkQueue, rmqDS);
    }
}
