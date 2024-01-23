package com.leomaster.wash;

import com.aiteer.springboot.common.vo.RmqConnectProperties;
import com.aiteer.springboot.stream.properties.StreamJobProperties;
import com.alibaba.fastjson.JSON;
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
import java.util.Objects;

/**
 * 用户注册数据过滤
 * 对用户注册数据进行去重（uid、当日有登录信息记录的只保留一条即可），
 */
public class WashUserApp {

    private final static Logger LOGGER = LoggerFactory.getLogger(WashUserApp.class);

    public static void setup(StreamExecutionEnvironment env, Map<String, DataStream<String>> linkedDSMap, StreamJobProperties properties) {
        StreamJobProperties.CdsRmq cdsRmq = properties.getCdsRmq();
        RmqConnectProperties sourceConnect = cdsRmq.getSource().getConnect();

        String sinkQueue = "wash_tb_user";
        RMQSource<String> rmqSource = MyRMQUtil.buildRmqSource(sourceConnect, cdsRmq.getSource().getQueueUserRegistry(), cdsRmq.getSource().getPrefetchCount());
        DataStream<String> jsonStrDS = env.addSource(rmqSource)
                .name("[source]: " + WashUserApp.class.getSimpleName());
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr))
                .name("userReg001_map");

        //考虑到同一个用户可能在不同的渠道进行登录，所以这里分组的key用uid和channel
        KeyedStream<JSONObject, Tuple2<String, String>> keybyWithUidDS = jsonObjDS.keyBy(
                new KeySelector<JSONObject, Tuple2<String, String>>(){
                    @Override
                    public Tuple2<String, String> getKey(JSONObject jsonObj) {
                        return Tuple2.of(jsonObj.getString("uid"), jsonObj.getString("channel"));
                    }
                }
        );

        SingleOutputStreamOperator<JSONObject> filterDS = keybyWithUidDS.filter(
                new TbUserUniqueFilter()
        ).name("userReg002_filter");

        SingleOutputStreamOperator<String> rmqDS = filterDS.map(jsonObj -> jsonObj.toJSONString())
                .name("userReg003_map");

        //尝试简化job的结构，直接存到map中，交给下一层的任务
        linkedDSMap.put(sinkQueue, rmqDS);
    }

    private static class TbUserUniqueFilter extends RichFilterFunction<JSONObject> {

        //定义状态
        ValueState<String> lastRegDateState = null;
        //定义日期工具类
        SimpleDateFormat sdf = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化日期工具类，这里只对比日期
            sdf = new SimpleDateFormat("yyyyMMdd");
            //初始化状态
            ValueStateDescriptor<String> lastVisitDateStateDes = new ValueStateDescriptor<>("lastRegDateState", String.class);

            lastVisitDateStateDes.enableTimeToLive(StateTtlConfigSingle.getInstance());
            this.lastRegDateState = getRuntimeContext().getState(lastVisitDateStateDes);
        }

        @Override
        public boolean filter(JSONObject jsonObj) throws Exception {
            if( !jsonObj.containsKey("ctime") || Objects.isNull(jsonObj.getLong("ctime"))){
                LOGGER.debug("[wash user app],{}",jsonObj);
                return false;
            }
            Long regTime = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), jsonObj.getString("pn"));
            String uid = jsonObj.getString("uid");
            String regDate = sdf.format(new Date(regTime));
            //获取状态日期
            String lastRecordDate = lastRegDateState.value();

            //用当前登录的访问时间和状态时间进行对比
            if (lastRecordDate != null && lastRecordDate.length() > 0 && lastRecordDate.equals(regDate)) {
                LOGGER.debug("user recorded, uid={}, lastRegDate={}, regDate ={}", uid, lastRecordDate, regDate);
                return false;
            } else {
                LOGGER.debug("user not recorded, uid={}, lastRegDate={}, regDate ={}", uid, lastRecordDate, regDate);
                lastRegDateState.update(regDate);
                return true;
            }
        }
    }

}
