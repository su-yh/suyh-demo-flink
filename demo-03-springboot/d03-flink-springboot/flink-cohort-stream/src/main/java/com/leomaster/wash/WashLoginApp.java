package com.leomaster.wash;

import com.aiteer.springboot.common.vo.RmqConnectProperties;
import com.aiteer.springboot.stream.properties.StreamJobProperties;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.single.StateTtlConfigSingle;
import com.leomaster.utils.DateTimeUtil;
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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * 登录数据过滤
 * 对用户登录数据进行去重（uid、当日有登录信息记录的只保留一条即可），
 *
 */
public class WashLoginApp {

    private final static Logger LOGGER = LoggerFactory.getLogger(WashLoginApp.class);

    public static void setup(StreamExecutionEnvironment env, Map<String, DataStream<String>> linkedDSMap, StreamJobProperties properties) {
        StreamJobProperties.CdsRmq cdsRmq = properties.getCdsRmq();
        RmqConnectProperties sourceConnect = cdsRmq.getSource().getConnect();

        String sinkQueue = "wash_tb_user_login";
        RMQSource<String> rmqSource = MyRMQUtil.buildRmqSource(sourceConnect, cdsRmq.getSource().getQueueUserLogin(), cdsRmq.getSource().getPrefetchCount());

        DataStream<String> jsonStrDS = env.addSource(rmqSource)
                .name("[source]: " + WashLoginApp.class.getSimpleName());

        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(JSON::parseObject)
                .name("userLogin001_map");


        KeyedStream<JSONObject, Tuple2<String, String>> keybyWithUidDS = jsonObjDS.keyBy(
                new KeySelector<JSONObject, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(JSONObject jsonObj) {
                        return Tuple2.of(jsonObj.getString("uid"), jsonObj.getString("channel"));
                    }
                }
        );

        SingleOutputStreamOperator<JSONObject> filterDS = keybyWithUidDS.filter(
                new TbUserLoginUniqueFilter()
        ).name("userLogin002_filter");

        SingleOutputStreamOperator<String> rmqDS = filterDS.map(JSONAware::toJSONString)
                .name("userLogin003_map");

        //尝试简化job的结构，直接存到map中，交给下一层的任务
        linkedDSMap.put(sinkQueue, rmqDS);
    }

    private static class TbUserLoginUniqueFilter extends RichFilterFunction<JSONObject> {

        //定义状态
        ValueState<String> lastLoginDateState = null;
        //定义日期工具类
        SimpleDateFormat sdf = null;

        @Override
        public void open(Configuration parameters) {
            //初始化日期工具类，这里只对比日期
            sdf = new SimpleDateFormat("yyyyMMdd");
            //初始化状态
            ValueStateDescriptor<String> lastVisitDateStateDes = new ValueStateDescriptor<>("lastLoginDateState", String.class);
            lastVisitDateStateDes.enableTimeToLive(StateTtlConfigSingle.getInstance());
            this.lastLoginDateState = getRuntimeContext().getState(lastVisitDateStateDes);
        }

        @Override
        public boolean filter(JSONObject jsonObj) throws Exception {
            Long loginTime = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), jsonObj.getString("pn"));

            String uid = jsonObj.getString("uid");

            if (jsonObj.containsKey("from_user_patch")) {
                LOGGER.debug("handle data from user stream patch, uid {}", uid);
            }

            String logDate = sdf.format(new Date(loginTime));
            //获取状态日期
            String lastVisitDate = lastLoginDateState.value();

            //用当前登录的访问时间和状态时间进行对比
            if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                LOGGER.debug("login recorded, uid={}, lastLoginDate={}, logDate ={}", uid, lastVisitDate, logDate);
                return false;
            } else {
                LOGGER.debug("login not recorded, uid={}, lastLoginDate={}, logDate ={}", uid, lastVisitDate, logDate);
                lastLoginDateState.update(logDate);
                return true;
            }
        }
    }

}
