package com.leomaster.wash;

import com.aiteer.springboot.common.vo.RmqConnectProperties;
import com.aiteer.springboot.stream.properties.StreamJobProperties;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.single.StateTtlConfigSingle;
import com.leomaster.utils.MyRMQUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 用户充值数据过滤
 * 对用户充值数据进行去重（uid、当日有登录信息记录的只保留一条即可），
 * 去重后的数据发回给rmq的dwd_tb_recharge队列
 */
public class WashRechargeApp {

    private final static Logger LOGGER = LoggerFactory.getLogger(WashRechargeApp.class);

    public static void setup(StreamExecutionEnvironment env, Map<String, DataStream<String>> linkedDSMap, StreamJobProperties properties) {
        StreamJobProperties.CdsRmq cdsRmq = properties.getCdsRmq();
        RmqConnectProperties sourceConnect = cdsRmq.getSource().getConnect();

        String sinkQueue = "wash_tb_recharge";
        RMQSource<String> rmqSource = MyRMQUtil.buildRmqSource(sourceConnect, cdsRmq.getSource().getQueueUserRecharge(), cdsRmq.getSource().getPrefetchCount());
        DataStream<String> jsonStrDS = env.addSource(rmqSource)
                .name("[source]: " + WashRechargeApp.class.getSimpleName());

        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr))
                .name("recharge001_map");

        //
        KeyedStream<JSONObject, Tuple2<String, String>> keybyWithUidDS = jsonObjDS.keyBy(
                new KeySelector<JSONObject, Tuple2<String, String>>(){
                    @Override
                    public Tuple2<String, String> getKey(JSONObject jsonObj) {
                        return Tuple2.of(jsonObj.getString("uid"), jsonObj.getString("order"));
                    }
                }
        );

        SingleOutputStreamOperator<JSONObject> filterDS = keybyWithUidDS.filter(
                new TbRechargeUniqueFilter()
        ).name("recharge002_filter");

        SingleOutputStreamOperator<String> rmqDS = filterDS.map(jsonObj -> jsonObj.toJSONString())
                .name("recharge003_map");

        //尝试简化job的结构，直接存到map中，交给下一层的任务
        linkedDSMap.put(sinkQueue, rmqDS);
    }

    private static class TbRechargeUniqueFilter extends RichFilterFunction<JSONObject> {
        //定义状态
        ValueState<String> lastOrderState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态
            ValueStateDescriptor<String> lastVisitDateStateDes = new ValueStateDescriptor<>("lastRechargeOrderDateState", String.class);
            //
            lastVisitDateStateDes.enableTimeToLive(StateTtlConfigSingle.getInstance());
            lastOrderState = getRuntimeContext().getState(lastVisitDateStateDes);
        }

        @Override
        public boolean filter(JSONObject jsonObj) throws Exception {
            String rechargeOrder = jsonObj.getString("order");
            String uid = jsonObj.getString("uid");
            //获取状态日期
            String lastRecodedOrder = lastOrderState.value();

            //用当前登录的访问时间和状态时间进行对比
            if (lastRecodedOrder != null && lastRecodedOrder.length() > 0 && lastRecodedOrder.equals(rechargeOrder)) {
                LOGGER.debug("recharge recorded, uid={}, lastRecodedOrder={}, rechargeOrder={}", uid, lastRecodedOrder, rechargeOrder);
                return false;
            } else {
                LOGGER.debug("recharge not recorded, uid={}, lastRecodedOrder={}, rechargeOrder={}", uid, lastRecodedOrder, rechargeOrder);
                lastOrderState.update(rechargeOrder);
                return true;
            }
        }
    }

}
