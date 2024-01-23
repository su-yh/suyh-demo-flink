package com.leomaster.poly;


import com.aiteer.springboot.common.constants.ConstantUtils;
import com.aiteer.springboot.core.context.FlinkSpringContext;
import com.aiteer.springboot.stream.properties.StreamJobProperties;
import com.aiteer.springboot.stream.vo.RmqSinkStream;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.core.constants.PN;
import com.leomaster.core.util.TextUtils;
import com.leomaster.dto.CdsStatsDto;
import com.leomaster.func.DimAsyncFunction;
import com.leomaster.single.StateTtlConfigSingle;
import com.leomaster.utils.DateTimeUtil;
import com.leomaster.utils.MyRMQUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 聚合数据计算
 */
public class PolyEventsWideTable {


    private final static Logger LOGGER = LoggerFactory.getLogger(PolyEventsWideTable.class);

    public static void setup(Map<String, Object> configProperties, Map<String, DataStream<String>> linkedDSMap) {

        //数据源定义
        final String cdsStatsSinkQueue = "poly_stats_live";
        final String cdsDetailStatsSinkQueue = "poly_detail_stats_live";
        final String cdsChannelStatsSinkQueue = "poly_real_curve_live";


        final DataStream<String> tbUserDS = linkedDSMap.get("related_tb_user");
        final DataStream<String> tbUserLoginDS = linkedDSMap.get("related_tb_user_login");
        final DataStream<String> tbRechargeDS = linkedDSMap.get("related_tb_recharge");
        final DataStream<String> tbWithdrawalDS = linkedDSMap.get("related_tb_withdrawal");


        //todo 自然天 分流 recharge_retention
        SingleOutputStreamOperator<CdsStatsDto> tbUserStatsDS = tbUserDS.process(new ProcessFunction<String, CdsStatsDto>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<CdsStatsDto> out) {
                JSONObject jsonObj = JSON.parseObject(jsonStr);

                String channel = jsonObj.getString("channel");
                String adCampaignKey = jsonObj.getString("adCampaignKey");
                Long regDate = jsonObj.getLong("regDate"); //用户的注册日期

                if (adCampaignKey == null) {
                    LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                    adCampaignKey = "Organic";
                }

                String pn = jsonObj.getString("pn");
                if (TextUtils.isEmpty(pn)) {
                    pn = PN.AW.Name;
                }
                Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);

                //每条tb_user数据可认为一个新增用户
                CdsStatsDto cdsStats = CdsStatsDto.builder()
                        .regDate(regDate)
                        .channel(channel)
                        .ad_campaign_key(adCampaignKey)
                        .new_user_ct(1L)
                        .pn(pn)
                        .ts(ts).build();

                //向下游输出
                out.collect(cdsStats);
            }
        }).name("[Poly-01]-userReg108_process");

        //tb_user_login jsonString
        SingleOutputStreamOperator<CdsStatsDto> tbUserLoginStatsDS = tbUserLoginDS.process(new ProcessFunction<String, CdsStatsDto>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<CdsStatsDto> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);

                String pn = jsonObj.getString("pn");
                if (TextUtils.isEmpty(pn)) {
                    pn = PN.AW.Name;
                }

                String channel = jsonObj.getString("channel");
                String adCampaignKey = jsonObj.getString("adCampaignKey");
                Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);
                // 即 新增 = regDate，行为日期 = ts
                long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                long statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                long eventDate = jsonObj.getLong("loginDate");
                String hashTime = jsonObj.getString("hashTime");
                String userRegChannel = jsonObj.getString("userRegChannel");
                long newRechargeDate = jsonObj.getLong("newRechargeDate");
                if (adCampaignKey == null) {
                    LOGGER.warn("warning , adCampaignKey  is null, data = {}", jsonStr);
                    adCampaignKey = "Organic";
                }

                long longtime = DateTimeUtil.toHashLong(hashTime);
                // TODO later 这里可以分流出一条包含用户注册时间的数据，从何可以利用注册时间获取到推广详情需要统计的维度
                long new_user_recharge_cs_dayretention = 0L;

                new_user_recharge_cs_dayretention = (DateTimeUtil.deltaDays(eventDate, newRechargeDate));


                //次留，只有渠道号未变化并且注册日期为登录日期的前一天时，才计算为次留
                int retentionUserCountInc = (channel.equals(userRegChannel) && (DateTimeUtil.deltaDays(eventDate, regDate)) == 1) ? 1 : 0;
                int xRetentionUserCountInc = channel.equals(userRegChannel) ? 1 : 0;
                //新增付费用户次留，只有渠道号未变化 并且 充值日期 为登录日期的前一天时，才计算为次留
//                long newUserRechargeCountIncDayretention = new_user_recharge_cs_dayretention==(DateTimeUtil.deltaDays(longtime, newRechargeDate)) ? 1L : 0L;
                long newUserRechargeCountIncDayretention = 0L;
                if (newRechargeDate > 0L) {
                    newUserRechargeCountIncDayretention = 1L;
                }

                //每条tb_user_login数据可认为一个活跃用户数据
                CdsStatsDto cdsStats = CdsStatsDto.builder()
                        .regDate(newRechargeDate)
                        .loginDate(eventDate)
                        .channel(channel)
                        .ad_campaign_key(adCampaignKey)
                        .active_user_ct(1L)
                        .retention_user_ct(retentionUserCountInc)
                        .x_retention_user_ct(xRetentionUserCountInc)
                        .new_user_recharge_ct_dayretention(newUserRechargeCountIncDayretention)
                        .pn(pn)
                        .ts(ts).build();

                //向下游输出
                out.collect(cdsStats);
            }
        }).name("[Poly-01]-userLogin108_process");

        //tb_recharge jsonString
        //从tb_recharge流中我们要区分统计字段：
        /**
         *  1、充值人数
         *  2、充值总金额
         *  3、新增充值人数
         *  4、新增充值总金额
         *  5、老用户充值人数
         *  6、老用户充值金额
         */

        SingleOutputStreamOperator<CdsStatsDto> tbRechargeStatsDS = tbRechargeDS
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        out.collect(jsonObj);
                    }
                })
                .name("[Poly-01]-recharge108_process")
                .keyBy(jsonObj -> jsonObj.getString("uid")) //可能还需要by渠道进行分组
                .process(new KeyedProcessFunction<String, JSONObject, CdsStatsDto>() {
                    private transient MapState<String, Integer> recordedUidMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> recordedUidStateDes = new MapStateDescriptor<>("recorded-new-recharge-uid-map-state", String.class, Integer.class);
                        recordedUidStateDes.enableTimeToLive(StateTtlConfigSingle.getInstance());
                        recordedUidMapState = getRuntimeContext().getMapState(recordedUidStateDes);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<CdsStatsDto> out) throws Exception {
                        String uid = ctx.getCurrentKey();

                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.AW.Name;
                        }

                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);

                        if (adCampaignKey == null) {
                            LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                            adCampaignKey = "Organic";
                        }

                        long dynamicDimension = jsonObj.getLong("dynamicDimension"); //用于统计的充值的 动态24小时维度
                        long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                        long statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                        long rechargeDate = jsonObj.getLong("rechargeDate"); //充值发生的日期
                        long day = jsonObj.getLong("day");
                        double goodsAmt = jsonObj.getDouble("goodsAmt"); //充值金额

                        long newRechargeDate = jsonObj.getLong("newRechargeDate")==null?0L:jsonObj.getLong("newRechargeDate");

                        //如果多天
                        String recordKey = uid + "_" + channel + "_" + adCampaignKey + "_" + day + "_" + newRechargeDate;
                        boolean uidHasRecorded = recordedUidMapState.contains(recordKey);
                        recordedUidMapState.put(recordKey, 1);


                        //这里在计算新增用户充值的时候，还是严格按照user表中能找到注册时间的规则来计算
                        long rechargeCountInc = uidHasRecorded ? 0L : 1L;
                        double rechargeAmtInc = goodsAmt;
                        //todo 计算新增付费用户数

                        long newUserRechargeCountInc = uidHasRecorded ? 0L : newRechargeDate == rechargeDate ? 1L : 0L;
//                        long newUserRechargeCountInc = newRechargeDate == rechargeDate ? 1L : 0L;

                        double newUserRechargeAmtInc = regDate == rechargeDate ? goodsAmt : 0L;
                        long oldUserRechargeCountInc = uidHasRecorded ? 0L : (DateTimeUtil.deltaDays(rechargeDate, regDate)) == 1 ? 1L : 0L;
                        double oldUserRechargeAmtInc = regDate == rechargeDate ? 0L : goodsAmt;


                        //输出一条包含充值统计信息的记录
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
                                .regDate(newRechargeDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .ts(ts)
                                .recharge_user_ct(rechargeCountInc)
                                .recharge_amt(rechargeAmtInc)
                                .new_user_recharge_ct(newUserRechargeCountInc)
                                .new_user_recharge_amt(newUserRechargeAmtInc)
                                .old_user_recharge_ct(oldUserRechargeCountInc)
                                .old_user_recharge_amt(oldUserRechargeAmtInc)
                                .dynamicDimension(dynamicDimension)
                                .pn(pn)
                                .build();

                        //像下游算子输出
                        out.collect(cdsStats);
                    }
                }).name("[Poly-01]-recharge109_process");

        //tb_withdrawal
        /**
         *  1、提现人数
         *  2、提现总金额
         */
        SingleOutputStreamOperator<CdsStatsDto> tbWithdrawalStatsDS = tbWithdrawalDS
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        out.collect(jsonObj);
                    }
                })
                .name("[Poly-01]-withdrawal108_process")
                .keyBy(jsonObj -> jsonObj.getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, CdsStatsDto>() {
                    private transient MapState<String, Integer> recordedUidMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> recordedUidStateDes = new MapStateDescriptor<>("recorded-withdrawal-uid-map-state", String.class, Integer.class);
                        recordedUidStateDes.enableTimeToLive(StateTtlConfigSingle.getInstance());
                        recordedUidMapState = getRuntimeContext().getMapState(recordedUidStateDes);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<CdsStatsDto> out) throws Exception {
                        String uid = ctx.getCurrentKey();

                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.AW.Name;
                        }

                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);

                        if (adCampaignKey == null) {
                            LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                            adCampaignKey = "Organic";
                        }

                        long dynamicDimension = jsonObj.getLong("dynamicDimension"); //用于统计的提现动态24小时维度
                        long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                        long day = jsonObj.getLong("day"); //用户的注册日期
                        Long statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                        long withdrawalDate = jsonObj.getLong("withdrawalDate"); //提现发生的日期
                        double amount = jsonObj.getDouble("amount"); //提现金额
                        long newRechargeDate = jsonObj.getLong("newRechargeDate") == null ? 0L : jsonObj.getLong("newRechargeDate"); //用于统计的提现动态24小时维度

                        String recordKey = uid + "_" + channel + "_" + adCampaignKey + "_" + day + "_" + withdrawalDate;
                        boolean uidHasRecorded = recordedUidMapState.contains(recordKey);
                        recordedUidMapState.put(recordKey, 1);

                        long withdrawalUserCountInc = uidHasRecorded ? 0L : 1L;
                        double withdrawalAmtInc = amount;
//                        long newUserRechargeCountInc = uidHasRecorded ? 0L : regDate == withdrawalDate ? 1L : 0L;
//                        double newUserRechargeAmtInc = regDate == withdrawalDate ? goodsAmt : 0L;
//                        long oldUserRechargeCountInc = uidHasRecorded ? 0L : regDate == withdrawalDate ? 0L : 1L;
//                        double oldUserRechargeAmtInc = regDate == withdrawalDate ? 0L : goodsAmt;

                        //输出一条包含充值统计信息的记录
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
                                .regDate(statRegDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .ts(ts)
                                .withdrawal_user_ct(withdrawalUserCountInc)
                                .withdrawal_amt(withdrawalAmtInc)
                                .dynamicDimension(dynamicDimension)
                                .pn(pn)
                                .build();

                        //像下游算子输出
                        out.collect(cdsStats);
                    }
                }).name("[Poly-01]-withdrawal109_process");

        //TODO 对转换后的流进行合并
        DataStream<CdsStatsDto> unionDS = tbUserStatsDS.union(tbUserLoginStatsDS, tbRechargeStatsDS, tbWithdrawalStatsDS);

        //由于爱玩产品数据的加入且需要按照不同的时区进行统计，所以需要在这里对数据进行分流，让后续数据进入不同的时间窗口
        OutputTag<CdsStatsDto> hyCdsStatsOUT = new OutputTag<CdsStatsDto>("hy-cds-stats-out") {
        };
        OutputTag<CdsStatsDto> awCdsStatsOUT = new OutputTag<CdsStatsDto>("aw-cds-stats-out") {
        };
        SingleOutputStreamOperator<CdsStatsDto> splitByPNDS = unionDS.process(new ProcessFunction<CdsStatsDto, CdsStatsDto>() {
            @Override
            public void processElement(CdsStatsDto cdsStats, Context context, Collector<CdsStatsDto> collector) {
                if (PN.HY.Name.equals(cdsStats.getPn())) {
                    context.output(hyCdsStatsOUT, cdsStats);
                } else {
                    context.output(awCdsStatsOUT, cdsStats);
                }
                collector.collect(cdsStats);
            }
        }).name("[Poly-01]-union109_process-split");

        DataStream<CdsStatsDto> hyCdsStatsDS = splitByPNDS.getSideOutput(hyCdsStatsOUT);
        DataStream<CdsStatsDto> awCdsStatsDS = splitByPNDS.getSideOutput(awCdsStatsOUT);

        //分流之后，在根据产品进行开窗聚合
        statsByPN(hyCdsStatsDS, configProperties);
        statsByPN(awCdsStatsDS, configProperties);


        // todo hash_time 分流 real_curve 数据量级较大，先屏蔽
     /*   SingleOutputStreamOperator<CdsStatsDto> tbUserStatsDSRealCurve = tbUserDS.process(new ProcessFunction<String, CdsStatsDto>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<CdsStatsDto> out) {
                JSONObject jsonObj = JSON.parseObject(jsonStr);

                String channel = jsonObj.getString("channel");
                String adCampaignKey = jsonObj.getString("adCampaignKey");
                Long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                String hashTime = jsonObj.getString("hashTime"); //
                if (adCampaignKey == null) {
                    LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                    adCampaignKey = "Organic";
                }

                String pn = jsonObj.getString("pn");
                if (TextUtils.isEmpty(pn)) {
                    pn = PN.HY.Name;
                }
                Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);

                //每条tb_user数据可认为一个新增用户
                CdsStatsDto cdsStats = CdsStatsDto.builder()
                        .hashTime(hashTime)
                        .regDate(regDate)
                        .channel(channel)
                        .ad_campaign_key(adCampaignKey)
                        .new_user_ct(1L)
                        .pn(pn)
                        .ts(ts).build();

                //向下游输出
                out.collect(cdsStats);
            }
        });*/

        //tb_user_login jsonString
      /*  SingleOutputStreamOperator<CdsStatsDto> tbUserLoginStatsDSRealCurve = tbUserLoginDS.process(new ProcessFunction<String, CdsStatsDto>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<CdsStatsDto> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);

                String pn = jsonObj.getString("pn");
                if (TextUtils.isEmpty(pn)) {
                    pn = PN.HY.Name;
                }
                String hashTime = jsonObj.getString("hashTime"); //
                String channel = jsonObj.getString("channel");
                String adCampaignKey = jsonObj.getString("adCampaignKey");
                Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);
                // 即 新增 = regDate，行为日期 = ts
                long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                long statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                long eventDate = jsonObj.getLong("loginDate");
                String userRegChannel = jsonObj.getString("userRegChannel");
                long newRechargeDate = jsonObj.getLong("newRechargeDate");
                if (adCampaignKey == null) {
                    LOGGER.warn("warning , adCampaignKey  is null, data = {}", jsonStr);
                    adCampaignKey = "Organic";
                }

                // TODO later 这里可以分流出一条包含用户注册时间的数据，从何可以利用注册时间获取到推广详情需要统计的维度

                //次留，只有渠道号未变化并且注册日期为登录日期的前一天时，才计算为次留
                int retentionUserCountInc = (channel.equals(userRegChannel) && (DateTimeUtil.deltaDays(eventDate, regDate)) == 1) ? 1 : 0;
                int xRetentionUserCountInc = channel.equals(userRegChannel) ? 1 : 0;
                //新增付费用户次留，只有渠道号未变化 并且 充值日期 为登录日期的前一天时，才计算为次留
                long newUserRechargeCountIncDayretention = (channel.equals(userRegChannel) && (DateTimeUtil.deltaDays(eventDate, newRechargeDate)) == 1) ? 1L : 0L;

                //每条tb_user_login数据可认为一个活跃用户数据
                CdsStatsDto cdsStats = CdsStatsDto.builder()
                        .hashTime(hashTime)
                        .regDate(statRegDate)
                        .loginDate(eventDate)
                        .channel(channel)
                        .ad_campaign_key(adCampaignKey)
                        .active_user_ct(1L)
                        .retention_user_ct(retentionUserCountInc)
                        .x_retention_user_ct(xRetentionUserCountInc)
                        .new_user_recharge_ct_dayretention(newUserRechargeCountIncDayretention)
                        .pn(pn)
                        .ts(ts).build();

                //向下游输出
                out.collect(cdsStats);
            }
        });*/

        //tb_recharge jsonString
        //从tb_recharge流中我们要区分统计字段：
        /**
         *  1、充值人数
         *  2、充值总金额
         *  3、新增充值人数
         *  4、新增充值总金额
         *  5、老用户充值人数
         *  6、老用户充值金额
         */
        /*SingleOutputStreamOperator<CdsStatsDto> tbRechargeStatsDSRealCurve = tbRechargeDS
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        out.collect(jsonObj);
                    }
                })
                .keyBy(jsonObj -> jsonObj.getString("uid")) //可能还需要by渠道进行分组
                .process(new KeyedProcessFunction<String, JSONObject, CdsStatsDto>() {
                    MapState<String, Integer> recordedUidMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> recordedUidStateDes = new MapStateDescriptor<>("recorded-real-recharge-uid-map-state", String.class, Integer.class);
                        recordedUidStateDes.enableTimeToLive(StateTtlConfigSingle.getInstance());
                        recordedUidMapState = getRuntimeContext().getMapState(recordedUidStateDes);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<CdsStatsDto> out) throws Exception {
                        String uid = ctx.getCurrentKey();

                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.HY.Name;
                        }

                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);

                        if (adCampaignKey == null) {
                            LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                            adCampaignKey = "Organic";
                        }

                        long dynamicDimension = jsonObj.getLong("dynamicDimension"); //用于统计的充值的 动态24小时维度
                        long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                        long statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                        long rechargeDate = jsonObj.getLong("rechargeDate"); //充值发生的日期
                        double goodsAmt = jsonObj.getDouble("goodsAmt"); //充值金额

                        String hashTime = jsonObj.getString("hashTime"); //

                        //如果多天
                        String recordKey = uid + "_" + channel + "_" + hashTime;
                        boolean uidHasRecorded = recordedUidMapState.contains(recordKey);

                        recordedUidMapState.put(recordKey, 1);

                        //这里在计算新增用户充值的时候，还是严格按照user表中能找到注册时间的规则来计算
                        long rechargeCountInc = uidHasRecorded ? 0L : 1L;
                        double rechargeAmtInc = goodsAmt;
                        //todo 计算新增付费用户数
                        //Integer rechargeIncVar = DateTimeUtil.deltaDays(rechargeDate, regDate);
                        long newUserRechargeCountInc = uidHasRecorded ? 0L : regDate == rechargeDate ? 1L : 0L;

                        //Integer eventIncVar = DateTimeUtil.deltaDays(rechargeDate, eventDate);

                        //付费用户次留不在此实现
                        //long newUserRechargeCountIncDayretention = eventIncVar == 0 ? 1L : 0L;

                        double newUserRechargeAmtInc = regDate == rechargeDate ? goodsAmt : 0L;
                        double newUserRechargeAmtIncDayretention = (DateTimeUtil.deltaDays(rechargeDate, regDate)) == 1 ? goodsAmt : 0L;
                        long oldUserRechargeCountInc = uidHasRecorded ? 0L : (DateTimeUtil.deltaDays(rechargeDate, regDate)) == 1 ? 1L : 0L;
                        double oldUserRechargeAmtInc = regDate == rechargeDate ? 0L : goodsAmt;


                        //输出一条包含充值统计信息的记录
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
                                .hashTime(hashTime)
                                .regDate(statRegDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .ts(ts)
                                .recharge_user_ct(rechargeCountInc)
                                .recharge_amt(rechargeAmtInc)
                                .new_user_recharge_ct(newUserRechargeCountInc)
                                .new_user_recharge_amt(newUserRechargeAmtInc)
                                .old_user_recharge_ct(oldUserRechargeCountInc)
                                .old_user_recharge_amt(oldUserRechargeAmtInc)
                                *//*.new_user_recharge_ct_dayretention(newUserRechargeCountIncDayretention)
                                .new_user_recharge_amt_dayretention(newUserRechargeAmtIncDayretention)*//*
                                .dynamicDimension(dynamicDimension)
                                .pn(pn)
                                .build();

                        //像下游算子输出
                        out.collect(cdsStats);
                    }
                });*/


        //tb_withdrawal
        /**
         *  1、提现人数
         *  2、提现总金额
         */
       /* SingleOutputStreamOperator<CdsStatsDto> tbWithdrawalStatsDSRealCurve = tbWithdrawalDS
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        out.collect(jsonObj);
                    }
                })
                .keyBy(jsonObj -> jsonObj.getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, CdsStatsDto>() {
                    MapState<String, Integer> recordedUidMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> recordedUidStateDes = new MapStateDescriptor<>("recorded-withdrawal-uid-map-state", String.class, Integer.class);
                        recordedUidStateDes.enableTimeToLive(StateTtlConfigSingle.getInstance());
                        recordedUidMapState = getRuntimeContext().getMapState(recordedUidStateDes);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<CdsStatsDto> out) throws Exception {
                        String uid = ctx.getCurrentKey();

                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.HY.Name;
                        }

                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);

                        if (adCampaignKey == null) {
                            LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                            adCampaignKey = "Organic";
                        }
                        String hashTime = jsonObj.getString("hashTime"); //
                        long dynamicDimension = jsonObj.getLong("dynamicDimension"); //用于统计的提现动态24小时维度
                        long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                        Long statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                        long withdrawalDate = jsonObj.getLong("withdrawalDate"); //提现发生的日期
                        double amount = jsonObj.getDouble("amount"); //提现金额

                        String recordKey = uid + "_" + channel + "_" + hashTime;
                        boolean uidHasRecorded = recordedUidMapState.contains(recordKey);
                       // boolean uidHasRecorded = false;
                        recordedUidMapState.put(recordKey, 1);

                        long withdrawalUserCountInc = uidHasRecorded ? 0L : 1L;
                        double withdrawalAmtInc = amount;
//                        long newUserRechargeCountInc = uidHasRecorded ? 0L : regDate == withdrawalDate ? 1L : 0L;
//                        double newUserRechargeAmtInc = regDate == withdrawalDate ? goodsAmt : 0L;
//                        long oldUserRechargeCountInc = uidHasRecorded ? 0L : regDate == withdrawalDate ? 0L : 1L;
//                        double oldUserRechargeAmtInc = regDate == withdrawalDate ? 0L : goodsAmt;

                        //输出一条包含充值统计信息的记录
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
                                .hashTime(hashTime)
                                .regDate(statRegDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .ts(ts)
                                .withdrawal_user_ct(withdrawalUserCountInc)
                                .withdrawal_amt(withdrawalAmtInc)
                                .dynamicDimension(dynamicDimension)
                                .pn(pn)
                                .build();

                        //像下游算子输出
                        out.collect(cdsStats);
                    }
                });*/

        //TODO 对转换后的流进行合并  紧跟上面先注释
  /*      DataStream<CdsStatsDto> unionDSRealCurve = tbUserStatsDSRealCurve.union(tbUserLoginStatsDSRealCurve, tbRechargeStatsDSRealCurve, tbWithdrawalStatsDSRealCurve);

        //由于爱玩产品数据的加入且需要按照不同的时区进行统计，所以需要在这里对数据进行分流，让后续数据进入不同的时间窗口
        OutputTag<CdsStatsDto> hyCdsStatsOUTRealCurve = new OutputTag<CdsStatsDto>("hy-cds-stats-out-real-curve") {
        };
        OutputTag<CdsStatsDto> awCdsStatsOUTRealCurve = new OutputTag<CdsStatsDto>("aw-cds-stats-out-real-curve") {
        };
        SingleOutputStreamOperator<CdsStatsDto> splitByPNDSRealCurve = unionDSRealCurve.process(new ProcessFunction<CdsStatsDto, CdsStatsDto>() {
            @Override
            public void processElement(CdsStatsDto cdsStats, Context context, Collector<CdsStatsDto> collector) {
                if (PN.HY.Name.equals(cdsStats.getPn())) {
                    context.output(hyCdsStatsOUTRealCurve, cdsStats);
                } else {
                    context.output(awCdsStatsOUTRealCurve, cdsStats);
                }
                collector.collect(cdsStats);
            }
        });

        DataStream<CdsStatsDto> hyCdsStatsDSRealCurve = splitByPNDSRealCurve.getSideOutput(hyCdsStatsOUTRealCurve);
        DataStream<CdsStatsDto> awCdsStatsDSRealCurve = splitByPNDSRealCurve.getSideOutput(awCdsStatsOUTRealCurve);

        //分流之后，在根据产品进行开窗聚合
        statsByPNRealCurve(cdsStatsSinkQueue, cdsDetailStatsSinkQueue, cdsChannelStatsSinkQueue, hyCdsStatsDSRealCurve);
        statsByPNRealCurve(cdsStatsSinkQueue, cdsDetailStatsSinkQueue, cdsChannelStatsSinkQueue, awCdsStatsDSRealCurve);*/


        // todo dynamicDimension 分流 cost_calculate_trend
        SingleOutputStreamOperator<CdsStatsDto> tbUserStatsDSCalculateTrend = tbUserDS.process(new ProcessFunction<String, CdsStatsDto>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<CdsStatsDto> out) {
                JSONObject jsonObj = JSON.parseObject(jsonStr);

                String channel = jsonObj.getString("channel");
                String adCampaignKey = jsonObj.getString("adCampaignKey");
                Long regDate = jsonObj.getLong("statRegDate"); //用户的注册日期
                if (adCampaignKey == null) {
                    LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                    adCampaignKey = "Organic";
                }

                long dynamicDimension = -1L;
                if (jsonObj.containsKey("dynamicDimension") && jsonObj.getLong("dynamicDimension") != null) {
                    dynamicDimension = jsonObj.getLong("dynamicDimension");//用于统计的充值的 动态24小时维度
                }

                String pn = jsonObj.getString("pn");
                if (TextUtils.isEmpty(pn)) {
                    pn = PN.AW.Name;
                }
                Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);

                //每条tb_user数据可认为一个新增用户
                CdsStatsDto cdsStats = CdsStatsDto.builder()
                        .dynamicDimension(dynamicDimension)
                        .regDate(regDate)
                        .channel(channel)
                        .ad_campaign_key(adCampaignKey)
                        .new_user_ct(1L)
                        .pn(pn)
                        .ts(ts).build();

                //向下游输出
                out.collect(cdsStats);
            }
        }).name("[Poly-02]-userReg208_process");

        //tb_user_login jsonString
        SingleOutputStreamOperator<CdsStatsDto> tbUserLoginStatsDSCalculateTrend = tbUserLoginDS.process(new ProcessFunction<String, CdsStatsDto>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<CdsStatsDto> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);

                String pn = jsonObj.getString("pn");
                if (TextUtils.isEmpty(pn)) {
                    pn = PN.AW.Name;
                }
                long dynamicDimension = -1L;
                if (jsonObj.containsKey("dynamicDimension") && jsonObj.getLong("dynamicDimension") != null) {
                    dynamicDimension = jsonObj.getLong("dynamicDimension");//用于统计的充值的 动态24小时维度
                }

                String channel = jsonObj.getString("channel");
                String adCampaignKey = jsonObj.getString("adCampaignKey");
                Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);
                // 即 新增 = regDate，行为日期 = ts
                long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                long statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                long eventDate = jsonObj.getLong("loginDate");
                String userRegChannel = jsonObj.getString("userRegChannel");
                long newRechargeDate = jsonObj.getLong("newRechargeDate");
                if (adCampaignKey == null) {
                    LOGGER.warn("warning , adCampaignKey  is null, data = {}", jsonStr);
                    adCampaignKey = "Organic";
                }

                // TODO later 这里可以分流出一条包含用户注册时间的数据，从何可以利用注册时间获取到推广详情需要统计的维度

                //次留，只有渠道号未变化并且注册日期为登录日期的前一天时，才计算为次留
                int retentionUserCountInc = (channel.equals(userRegChannel) && (DateTimeUtil.deltaDays(eventDate, regDate)) == 1) ? 1 : 0;
                int xRetentionUserCountInc = channel.equals(userRegChannel) ? 1 : 0;
                //新增付费用户次留，只有渠道号未变化 并且充值日期 为登录日期的前一天时，才计算为次留
//                long newUserRechargeCountIncDayretention = (channel.equals(userRegChannel) && (DateTimeUtil.deltaDays(eventDate, newRechargeDate)) == 1) ? 1L : 0L;
                long newUserRechargeCountIncDayretention = 0L;
                if (newRechargeDate > 0L) {
                    newUserRechargeCountIncDayretention = 1L;
                }


                //每条tb_user_login数据可认为一个活跃用户数据
                CdsStatsDto cdsStats = CdsStatsDto.builder()
                        .dynamicDimension(dynamicDimension)
                        .regDate(statRegDate)
                        .loginDate(eventDate)
                        .channel(channel)
                        .ad_campaign_key(adCampaignKey)
                        .active_user_ct(1L)
                        .retention_user_ct(retentionUserCountInc)
                        .x_retention_user_ct(xRetentionUserCountInc)
                        .new_user_recharge_ct_dayretention(newUserRechargeCountIncDayretention)
                        .pn(pn)
                        .ts(ts).build();

                //向下游输出
                out.collect(cdsStats);
            }
        }).name("[Poly-02]-userLogin208_process");

        //tb_recharge jsonString
        //从tb_recharge流中我们要区分统计字段：
        /**
         *  1、充值人数
         *  2、充值总金额
         *  3、新增充值人数
         *  4、新增充值总金额
         *  5、老用户充值人数
         *  6、老用户充值金额
         */
        SingleOutputStreamOperator<CdsStatsDto> tbRechargeStatsDSCalculateTrend = tbRechargeDS
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        out.collect(jsonObj);
                    }
                })
                .name("[Poly-02]-recharge208_process")
                .keyBy(jsonObj -> jsonObj.getString("uid")) //可能还需要by渠道进行分组
                .process(new KeyedProcessFunction<String, JSONObject, CdsStatsDto>() {
                    MapState<String, Integer> recordedUidMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> recordedUidStateDes = new MapStateDescriptor<>("recorded-calculate-recharge-uid-map-state", String.class, Integer.class);
                        recordedUidStateDes.enableTimeToLive(StateTtlConfigSingle.getInstance());
                        recordedUidMapState = getRuntimeContext().getMapState(recordedUidStateDes);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<CdsStatsDto> out) throws Exception {
                        String uid = ctx.getCurrentKey();

                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.AW.Name;
                        }

                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);

                        if (adCampaignKey == null) {
                            LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                            adCampaignKey = "Organic";
                        }

                        long dynamicDimension = -1L;
                        if (jsonObj.containsKey("dynamicDimension") && jsonObj.getLong("dynamicDimension") != null) {
                            dynamicDimension = jsonObj.getLong("dynamicDimension");//用于统计的充值的 动态24小时维度
                        }

                        long day = jsonObj.getLong("day");
                        long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                        long statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                        long rechargeDate = jsonObj.getLong("rechargeDate"); //充值发生的日期
                        double goodsAmt = jsonObj.getDouble("goodsAmt"); //充值金额

                        //如果多天
                        String recordKey = uid + "_" + channel + "_" + dynamicDimension + "_" + adCampaignKey + "_" + regDate + "_" + day;
                        boolean uidHasRecorded = recordedUidMapState.contains(recordKey);
                        recordedUidMapState.put(recordKey, 1);

                        //这里在计算新增用户充值的时候，还是严格按照user表中能找到注册时间的规则来计算
                        long rechargeCountInc = uidHasRecorded ? 0L : 1L;
                        double rechargeAmtInc = goodsAmt;
                        //todo 计算新增付费用户数

                        long newUserRechargeCountInc = uidHasRecorded ? 0L : regDate == rechargeDate ? 1L : 0L;

                        double newUserRechargeAmtInc = regDate == rechargeDate ? goodsAmt : 0L;
                        long oldUserRechargeCountInc = uidHasRecorded ? 0L : (DateTimeUtil.deltaDays(rechargeDate, regDate)) == 1 ? 1L : 0L;
                        double oldUserRechargeAmtInc = regDate == rechargeDate ? 0L : goodsAmt;

                        LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());

                        //输出一条包含充值统计信息的记录
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
                                .regDate(statRegDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .ts(ts)
                                .recharge_user_ct(rechargeCountInc)
                                .recharge_amt(rechargeAmtInc)
                                .new_user_recharge_ct(newUserRechargeCountInc)
                                .new_user_recharge_amt(newUserRechargeAmtInc)
                                .old_user_recharge_ct(oldUserRechargeCountInc)
                                .old_user_recharge_amt(oldUserRechargeAmtInc)
                                .dynamicDimension(dynamicDimension)
                                .pn(pn)
                                .build();

                        //像下游算子输出
                        out.collect(cdsStats);
                    }
                }).name("[Poly-02]-recharge208_process");


        //tb_withdrawal
        /**
         *  1、提现人数
         *  2、提现总金额
         */
        SingleOutputStreamOperator<CdsStatsDto> tbWithdrawalStatsDSCalculateTrend = tbWithdrawalDS
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        out.collect(jsonObj);
                    }
                })
                .name("[Poly-02]-withdrawal208_process")
                .keyBy(jsonObj -> jsonObj.getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, CdsStatsDto>() {
                    MapState<String, Integer> recordedUidMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> recordedUidStateDes = new MapStateDescriptor<>("recorded-withdrawal-uid-map-state", String.class, Integer.class);
                        recordedUidStateDes.enableTimeToLive(StateTtlConfigSingle.getInstance());
                        recordedUidMapState = getRuntimeContext().getMapState(recordedUidStateDes);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<CdsStatsDto> out) throws Exception {
                        String uid = ctx.getCurrentKey();

                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.AW.Name;
                        }

                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);

                        if (adCampaignKey == null) {
                            LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                            adCampaignKey = "Organic";
                        }

                        long dynamicDimension = -1L;
                        if (jsonObj.containsKey("dynamicDimension") && jsonObj.getLong("dynamicDimension") != null) {
                            dynamicDimension = jsonObj.getLong("dynamicDimension");//用于统计的充值的 动态24小时维度
                        }
                        long day = jsonObj.getLong("day"); //用户的注册日期

                        long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                        Long statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                        long withdrawalDate = jsonObj.getLong("withdrawalDate"); //提现发生的日期
                        double amount = jsonObj.getDouble("amount"); //提现金额

                        String recordKey = uid + "_" + channel + "_" + dynamicDimension + "_" + adCampaignKey + "_" + regDate + "_" + day;
                        boolean uidHasRecorded = recordedUidMapState.contains(recordKey);
                        recordedUidMapState.put(recordKey, 1);

                        long withdrawalUserCountInc = uidHasRecorded ? 0L : 1L;
                        double withdrawalAmtInc = amount;
//                        long newUserRechargeCountInc = uidHasRecorded ? 0L : regDate == withdrawalDate ? 1L : 0L;
//                        double newUserRechargeAmtInc = regDate == withdrawalDate ? goodsAmt : 0L;
//                        long oldUserRechargeCountInc = uidHasRecorded ? 0L : regDate == withdrawalDate ? 0L : 1L;
//                        double oldUserRechargeAmtInc = regDate == withdrawalDate ? 0L : goodsAmt;

                        //输出一条包含充值统计信息的记录
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
                                .regDate(statRegDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .ts(ts)
                                .withdrawal_user_ct(withdrawalUserCountInc)
                                .withdrawal_amt(withdrawalAmtInc)
                                .dynamicDimension(dynamicDimension)
                                .pn(pn)
                                .build();

                        //像下游算子输出
                        out.collect(cdsStats);
                    }
                }).name("[Poly-02]-withdrawal209_process");

        //TODO 对转换后的流进行合并
        DataStream<CdsStatsDto> unionDSCalculateTrend = tbUserStatsDSCalculateTrend.union(tbUserLoginStatsDSCalculateTrend, tbRechargeStatsDSCalculateTrend, tbWithdrawalStatsDSCalculateTrend);

        //由于爱玩产品数据的加入且需要按照不同的时区进行统计，所以需要在这里对数据进行分流，让后续数据进入不同的时间窗口
        OutputTag<CdsStatsDto> hyCdsStatsOUTCalculateTrend = new OutputTag<CdsStatsDto>("hy-cds-stats-out-calculate-trend") {
        };
        OutputTag<CdsStatsDto> awCdsStatsOUTCalculateTrend = new OutputTag<CdsStatsDto>("aw-cds-stats-out-calculate-trend") {
        };
        SingleOutputStreamOperator<CdsStatsDto> splitByPNDSCalculateTrend = unionDSCalculateTrend.process(new ProcessFunction<CdsStatsDto, CdsStatsDto>() {
            @Override
            public void processElement(CdsStatsDto cdsStats, Context context, Collector<CdsStatsDto> collector) {
                if (PN.HY.Name.equals(cdsStats.getPn())) {
                    context.output(hyCdsStatsOUTCalculateTrend, cdsStats);
                } else {
                    context.output(awCdsStatsOUTCalculateTrend, cdsStats);
                }
                collector.collect(cdsStats);
            }
        }).name("[Poly-02]-union209_process");

        DataStream<CdsStatsDto> hyCdsStatsDSCalculateTrend = splitByPNDSCalculateTrend.getSideOutput(hyCdsStatsOUTCalculateTrend);
        DataStream<CdsStatsDto> awCdsStatsDSCalculateTrend = splitByPNDSCalculateTrend.getSideOutput(awCdsStatsOUTCalculateTrend);

        //分流之后，在根据产品进行开窗聚合
        statsByPNCalculateTrend(configProperties, hyCdsStatsDSCalculateTrend);
        statsByPNCalculateTrend(configProperties, awCdsStatsDSCalculateTrend);
    }


//    private static void statsByPNRealCurve(String cdsStatsSinkQueue, String cdsDetailStatsSinkQueue, String cdsChannelStatsSinkQueue, DataStream<CdsStatsDto> xxCdsStatsDS) {
//
//        // 提取时间、设置watermark
//        SingleOutputStreamOperator<CdsStatsDto> cdsStatsWithWatermarkDS = xxCdsStatsDS.assignTimestampsAndWatermarks(
//                WatermarkStrategy.<CdsStatsDto>forBoundedOutOfOrderness(Duration.ofMillis(1))
//                        .withTimestampAssigner((SerializableTimestampAssigner<CdsStatsDto>) (cdsStats, l) -> cdsStats.getTs()));
//
//        // 按照维度对数据进行分组
//        KeyedStream<CdsStatsDto, Tuple3<String, String, String>> cdsStatsKeyedByChannelDS = cdsStatsWithWatermarkDS.keyBy(
//                new KeySelector<CdsStatsDto, Tuple3<String, String, String>>() {
//                    @Override
//                    public Tuple3<String, String, String> getKey(CdsStatsDto cdsStats) throws Exception {
//                        return Tuple3.of(cdsStats.getChannel(), cdsStats.getAd_campaign_key(), cdsStats.getHashTime());
//                    }
//                });
//
//
//        // 开窗后聚合 按推广素材统计
//       /* WindowedStream<CdsStatsDto, Tuple3<String, String,String>, TimeWindow> cdsStatsWindowedByChannelDS = cdsStatsKeyedByChannelDS
//                .window(TumblingEventTimeWindows.of(Time.minutes(10), Time.minutes(ConstantUtils.WATERMARKS_OFFSET_HOURUTC))) //滚动窗口 周期，offset考虑时区
//                .allowedLateness(Time.minutes(2)) //允许迟到数据
//                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(ConstantUtils.STAT_WINDOW_TRIGGER_SECS * 3)));
//            *///触发窗口计算
//
//        WindowedStream<CdsStatsDto, Tuple3<String, String, String>, TimeWindow> cdsStatsWindowedByChannelDS = cdsStatsKeyedByChannelDS
//                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(ConstantUtils.WATERMARKS_OFFSET_HOURUTC))) //滚动窗口 周期，offset考虑时区
//                .allowedLateness(Time.hours(2)) //允许迟到数据
//                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(ConstantUtils.STAT_WINDOW_TRIGGER_SECS * ConstantUtils.STAT_WINDOW_TRIGGER_SECS_TIME)));
//        // TODO: suyh - 内网测试时窗口调小
////                .window(TumblingEventTimeWindows.of(Time.minutes(10), Time.hours(ConstantUtils.WATERMARKS_OFFSET_HOURUTC))) //滚动窗口 周期，offset考虑时区
////                .allowedLateness(Time.minutes(2)) //允许迟到数据
////                .trigger(ContinuousProcessingTimeTrigger.of(Time.minutes(1)));
//
//        SingleOutputStreamOperator<CdsStatsDto> cdsStatsByChannelDS = cdsStatsWindowedByChannelDS.reduce(
//                new CdsStatsReduceFunction(),
//                new CdsStatsByChannelProcessWindowFunction()
//        );
//
//
//        // 更详细的维度关联，比如广告素材组id等...
//        SingleOutputStreamOperator<CdsStatsDto> cdsStatsByChannelWideDS = AsyncDataStream.unorderedWait(
//                cdsStatsByChannelDS,
//                new DimAsyncFunction<CdsStatsDto>("adjust_ad") {
//                    @Override
//                    public Tuple2<String, String>[] getCondition(CdsStatsDto obj) {
//                        return new Tuple2[]{Tuple2.of("`key`", obj.getAd_campaign_key())};
//                    }
//
//                    @Override
//                    public void join(CdsStatsDto obj, JSONObject dimInfoJsonObj) {
//                        obj.setSource(dimInfoJsonObj.getString("source"));
//                        obj.setPkg(dimInfoJsonObj.getString("pkg"));
//                        obj.setIs_organic(dimInfoJsonObj.getLong("isOrganic"));
//
//                        obj.setGoogleAdsCampaignId(dimInfoJsonObj.getString("googleAdsCampaignId"));
//                        obj.setGoogleAdsCampaignName(dimInfoJsonObj.getString("googleAdsCampaignName"));
//                        obj.setGoogleAdsAdgroupId(dimInfoJsonObj.getString("googleAdsAdgroupId"));
//                        obj.setGoogleAdsAdgroupName(dimInfoJsonObj.getString("googleAdsAdgroupName"));
//                        obj.setGoogleAdsCreativeId(dimInfoJsonObj.getString("googleAdsCreativeId"));
//                        obj.setGoogleAdsCampaignType(dimInfoJsonObj.getString("googleAdsCampaignType"));
//
//                        obj.setFbCampaignGroupName(dimInfoJsonObj.getString("fbCampaignGroupName"));
//                        obj.setFbCampaignGroupId(dimInfoJsonObj.getString("fbCampaignGroupId"));
//                        obj.setFbCampaignName(dimInfoJsonObj.getString("fbCampaignName"));
//                        obj.setFbCampaignId(dimInfoJsonObj.getString("fbCampaignId"));
//                        obj.setFbAdgroupName(dimInfoJsonObj.getString("fbAdgroupName"));
//                        obj.setFbAdgroupId(dimInfoJsonObj.getString("fbAdgroupId"));
//                    }
//
//                    @Override
//                    public void dimFailed(CdsStatsDto obj) {
//                        //没有匹配到的Campaign信息，当做Organic处理
//                        obj.setSource("Organic");
//                        obj.setIs_organic(1L);
//
//                        LOGGER.warn("warning, adjust_ad dim failed by ad, stats: {}", obj);
//                    }
//                }, ConstantUtils.DIM_TIMEOUT_SECONDS, TimeUnit.SECONDS, ConstantUtils.DIM_CAPACITY
//        );
//
//        //hash_time 分流汇总输出
//        cdsStatsByChannelWideDS
//                .map(cdsStats -> JSON.toJSONString(cdsStats))
//                .addSink(MyRMQUtil.getRMQSink(cdsChannelStatsSinkQueue));
//    }


    private static void statsByPN(DataStream<CdsStatsDto> xxCdsStatsDS, Map<String, Object> configProperties) {

        // 提取时间、设置watermark
        SingleOutputStreamOperator<CdsStatsDto> cdsStatsWithWatermarkDS = xxCdsStatsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<CdsStatsDto>forBoundedOutOfOrderness(Duration.ofMillis(1))
                        .withTimestampAssigner((SerializableTimestampAssigner<CdsStatsDto>) (cdsStats, l) -> cdsStats.getTs()))
                .name("[Poly-01]-union110_tsWatermark");

        // 按照维度对数据进行分组
        KeyedStream<CdsStatsDto, Tuple3<String, String, Long>> cdsStatsKeyedByAdDS = cdsStatsWithWatermarkDS.keyBy(
                new KeySelector<CdsStatsDto, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> getKey(CdsStatsDto cdsStats) throws Exception {
                        return Tuple3.of(cdsStats.getChannel(), cdsStats.getAd_campaign_key(), cdsStats.getRegDate());
                    }
                });


        StreamJobProperties properties = FlinkSpringContext.getBean(StreamJobProperties.class);

        // 开窗后聚合 按推广素材统计
        WindowedStream<CdsStatsDto, Tuple3<String, String, Long>, TimeWindow> cdsStatsWindowedByAdDS = cdsStatsKeyedByAdDS
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(ConstantUtils.WATERMARKS_OFFSET_HOURUTC))) //滚动窗口
                .allowedLateness(Time.hours(2)) //允许迟到数据3个小时
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(properties.getCdsRuntime().getTimeTriggerSecond())));     //10s触发一次窗口计算
        // TODO: suyh - 内网测试时窗口调小
//                .window(TumblingEventTimeWindows.of(Time.minutes(10), Time.hours(ConstantUtils.WATERMARKS_OFFSET_HOURUTC))) //滚动窗口
//                .allowedLateness(Time.minutes(2)) //允许迟到数据3个小时
//                .trigger(ContinuousProcessingTimeTrigger.of(Time.minutes(1)));

        SingleOutputStreamOperator<CdsStatsDto> reduceByAdDS = cdsStatsWindowedByAdDS.reduce(
                new CdsStatsReduceFunction(),
                new CdsStatsByAdProcessWindowFunction()
        ).name("[Poly-01]-union111_reduce");

        // 更详细的维度关联，比如广告素材组id等...
        SingleOutputStreamOperator<CdsStatsDto> cdsStatsWithAdsWideDS = AsyncDataStream.unorderedWait(
                reduceByAdDS,
                new DimAsyncFunction<CdsStatsDto>("adjust_ad", configProperties) {
                    @Override
                    public Tuple2<String, String>[] getCondition(CdsStatsDto obj) {
                        return new Tuple2[]{Tuple2.of("`key`", obj.getAd_campaign_key())};
                    }

                    @Override
                    public void join(CdsStatsDto obj, JSONObject dimInfoJsonObj) {
                        obj.setSource(dimInfoJsonObj.getString("source"));
                        obj.setPkg(dimInfoJsonObj.getString("pkg"));
                        obj.setIs_organic(dimInfoJsonObj.getLong("isOrganic"));

                        obj.setGoogleAdsCampaignId(dimInfoJsonObj.getString("googleAdsCampaignId"));
                        obj.setGoogleAdsCampaignName(dimInfoJsonObj.getString("googleAdsCampaignName"));
                        obj.setGoogleAdsAdgroupId(dimInfoJsonObj.getString("googleAdsAdgroupId"));
                        obj.setGoogleAdsAdgroupName(dimInfoJsonObj.getString("googleAdsAdgroupName"));
                        obj.setGoogleAdsCreativeId(dimInfoJsonObj.getString("googleAdsCreativeId"));
                        obj.setGoogleAdsCampaignType(dimInfoJsonObj.getString("googleAdsCampaignType"));

                        obj.setFbCampaignGroupName(dimInfoJsonObj.getString("fbCampaignGroupName"));
                        obj.setFbCampaignGroupId(dimInfoJsonObj.getString("fbCampaignGroupId"));
                        obj.setFbCampaignName(dimInfoJsonObj.getString("fbCampaignName"));
                        obj.setFbCampaignId(dimInfoJsonObj.getString("fbCampaignId"));
                        obj.setFbAdgroupName(dimInfoJsonObj.getString("fbAdgroupName"));
                        obj.setFbAdgroupId(dimInfoJsonObj.getString("fbAdgroupId"));
                    }

                    @Override
                    public void dimFailed(CdsStatsDto obj) {
                        //没有匹配到的Campaign信息，当做Organic处理
                        obj.setSource("Organic");
                        obj.setIs_organic(1L);

                        LOGGER.warn("warning, adjust_ad dim failed by ad, stats: {}", obj);
                    }
                }, ConstantUtils.DIM_TIMEOUT_SECONDS, TimeUnit.SECONDS, ConstantUtils.DIM_CAPACITY
        ).name("[Poly-01]-union112_asyncDataStream");

        StreamJobProperties.CdsRmq cdsRmq = properties.getCdsRmq();
        RmqSinkStream rmqSink = cdsRmq.getSink();
        //todo 自然天汇总统计
        // 暂时关闭 存在双推问题
        cdsStatsWithAdsWideDS
                .map(cdsStats -> JSON.toJSONString(cdsStats))
                .addSink(MyRMQUtil.buildRmqSink(rmqSink.getConnect(), rmqSink.getExchange(), rmqSink.getRoutingKeyCohort()))
                .name("[Poly-01]-union113_sink");
    }

    //todo dynamicDimension 分流 cost_calculate_trend
    private static void statsByPNCalculateTrend(Map<String, Object> configProperties, DataStream<CdsStatsDto> xxCdsStatsDS) {


        // 提取时间、设置watermark
        SingleOutputStreamOperator<CdsStatsDto> cdsStatsWithWatermarkDS = xxCdsStatsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<CdsStatsDto>forBoundedOutOfOrderness(Duration.ofMillis(1))
                        .withTimestampAssigner((SerializableTimestampAssigner<CdsStatsDto>) (cdsStats, l) -> cdsStats.getTs()))
                .name("[Poly-02]-union210_tsWatermark");


        // 按渠道+新增日期+广告素材详情分组计算
        KeyedStream<CdsStatsDto, Tuple4<String, String, Long, Long>> cdsStatsKeyedByAdDetailDS = cdsStatsWithWatermarkDS.keyBy(
                new KeySelector<CdsStatsDto, Tuple4<String, String, Long, Long>>() {
                    @Override
                    public Tuple4<String, String, Long, Long> getKey(CdsStatsDto cdsStats) throws Exception {
                        return Tuple4.of(cdsStats.getChannel(), cdsStats.getAd_campaign_key(), cdsStats.getRegDate(), cdsStats.getDynamicDimension());
                    }
                });

        StreamJobProperties properties = FlinkSpringContext.getBean(StreamJobProperties.class);
        // 开窗后聚合 按推广详细数据计算
        WindowedStream<CdsStatsDto, Tuple4<String, String, Long, Long>, TimeWindow> cdsStatsWindowedByAdDetailDS = cdsStatsKeyedByAdDetailDS
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(ConstantUtils.WATERMARKS_OFFSET_HOURUTC))) //滚动窗口
                .allowedLateness(Time.hours(2)) //允许迟到数据2个小时
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(properties.getCdsRuntime().getTimeTriggerSecond())));
        // TODO: suyh - 内网测试时窗口调小
//                .window(TumblingEventTimeWindows.of(Time.minutes(10), Time.hours(ConstantUtils.WATERMARKS_OFFSET_HOURUTC))) //滚动窗口
//                .allowedLateness(Time.minutes(2)) //允许迟到数据2个小时
//                .trigger(ContinuousProcessingTimeTrigger.of(Time.minutes(1)));

        SingleOutputStreamOperator<CdsStatsDto> reduceByAdDetailDS = cdsStatsWindowedByAdDetailDS.reduce(
                new CdsStatsReduceFunction(),
                new CdsStatsByAdDetailProcessWindowFunction()
        ).name("[Poly-02]-union211_reduce");

        // 更详细的维度关联，比如广告素材组id等...
        SingleOutputStreamOperator<CdsStatsDto> cdsStatsWithAdsDetailWideDS = AsyncDataStream.unorderedWait(
                reduceByAdDetailDS,
                new DimAsyncFunction<CdsStatsDto>("adjust_ad", configProperties) {
                    @Override
                    public Tuple2<String, String>[] getCondition(CdsStatsDto obj) {
                        return new Tuple2[]{Tuple2.of("`key`", obj.getAd_campaign_key())};
                    }

                    @Override
                    public void join(CdsStatsDto obj, JSONObject dimInfoJsonObj) throws Exception {
                        obj.setSource(dimInfoJsonObj.getString("source"));
                        obj.setPkg(dimInfoJsonObj.getString("pkg"));
                        obj.setIs_organic(dimInfoJsonObj.getLong("isOrganic"));

                        obj.setGoogleAdsCampaignId(dimInfoJsonObj.getString("googleAdsCampaignId"));
                        obj.setGoogleAdsCampaignName(dimInfoJsonObj.getString("googleAdsCampaignName"));
                        obj.setGoogleAdsAdgroupId(dimInfoJsonObj.getString("googleAdsAdgroupId"));
                        obj.setGoogleAdsAdgroupName(dimInfoJsonObj.getString("googleAdsAdgroupName"));
                        obj.setGoogleAdsCreativeId(dimInfoJsonObj.getString("googleAdsCreativeId"));
                        obj.setGoogleAdsCampaignType(dimInfoJsonObj.getString("googleAdsCampaignType"));

                        obj.setFbCampaignGroupName(dimInfoJsonObj.getString("fbCampaignGroupName"));
                        obj.setFbCampaignGroupId(dimInfoJsonObj.getString("fbCampaignGroupId"));
                        obj.setFbCampaignName(dimInfoJsonObj.getString("fbCampaignName"));
                        obj.setFbCampaignId(dimInfoJsonObj.getString("fbCampaignId"));
                        obj.setFbAdgroupName(dimInfoJsonObj.getString("fbAdgroupName"));
                        obj.setFbAdgroupId(dimInfoJsonObj.getString("fbAdgroupId"));
                    }

                    @Override
                    public void dimFailed(CdsStatsDto obj) {
                        //没有匹配到的Campaign信息，当做Organic处理
                        obj.setSource("Organic");
                        obj.setIs_organic(1L);

                        LOGGER.warn("warning, adjust_ad dim failed by detail, stats: {}", obj);
                    }
                }, ConstantUtils.DIM_TIMEOUT_SECONDS, TimeUnit.SECONDS, ConstantUtils.DIM_CAPACITY
        ).name("[Poly-02]-union212_async");

        //汇总数据写回MQ，由其他的程序消费并更新计算结果

        StreamJobProperties.CdsRmq cdsRmq = properties.getCdsRmq();
        RmqSinkStream rmqSink = cdsRmq.getSink();

        try{
            // topic 方式发送
            cdsStatsWithAdsDetailWideDS
                    .map(cdsStats -> JSON.toJSONString(cdsStats))
                    .addSink(MyRMQUtil.buildRmqSink(rmqSink.getConnect(), rmqSink.getExchange(), rmqSink.getRoutingKeyPaidRetention()))
                    .name("[Poly-02]-union212_sink");
        }catch (Exception e){
            LOGGER.error("[Exception]",e);
        }
    }


    /**
     * 统计数据聚合的函数，把之前分类的值进行相加，三种分组方式可以共用该聚合函数
     */
    public static class CdsStatsReduceFunction implements ReduceFunction<CdsStatsDto> {
        @Override
        public CdsStatsDto reduce(CdsStatsDto stats1, CdsStatsDto stats2) throws Exception {
            stats1.setNew_user_ct(stats1.getNew_user_ct() + stats2.getNew_user_ct());
            stats1.setActive_user_ct(stats1.getActive_user_ct() + stats2.getActive_user_ct());
            stats1.setRetention_user_ct(stats1.getRetention_user_ct() + stats2.getRetention_user_ct());
            stats1.setX_retention_user_ct(stats1.getX_retention_user_ct() + stats2.getX_retention_user_ct());
            stats1.setRecharge_user_ct(stats1.getRecharge_user_ct() + stats2.getRecharge_user_ct());
            stats1.setRecharge_amt(stats1.getRecharge_amt() + stats2.getRecharge_amt());
            stats1.setNew_user_recharge_ct(stats1.getNew_user_recharge_ct() + stats2.getNew_user_recharge_ct());
            stats1.setNew_user_recharge_ct_dayretention(stats1.getNew_user_recharge_ct_dayretention() + stats2.getNew_user_recharge_ct_dayretention());
            stats1.setNew_user_recharge_amt_dayretention(stats1.getNew_user_recharge_amt_dayretention() + stats2.getNew_user_recharge_amt_dayretention());
            stats1.setNew_user_recharge_amt(stats1.getNew_user_recharge_amt() + stats2.getNew_user_recharge_amt());
            stats1.setOld_user_recharge_ct(stats1.getOld_user_recharge_ct() + stats2.getOld_user_recharge_ct());
            stats1.setOld_user_recharge_amt(stats1.getOld_user_recharge_amt() + stats2.getOld_user_recharge_amt());
            stats1.setWithdrawal_user_ct(stats1.getWithdrawal_user_ct() + stats2.getWithdrawal_user_ct());
            stats1.setWithdrawal_amt(stats1.getWithdrawal_amt() + stats2.getWithdrawal_amt());
            return stats1;
        }
    }


    public static class CdsStatsByChannelProcessWindowFunction extends ProcessWindowFunction<CdsStatsDto, CdsStatsDto, Tuple3<String, String, String>, TimeWindow> {

        @Override
        public void process(Tuple3<String, String, String> keys, Context ctx, Iterable<CdsStatsDto> elements, Collector<CdsStatsDto> out) throws Exception {

            for (CdsStatsDto cdsStats : elements) {

                Date date = new Date();
                cdsStats.setChannel(keys.f0);
                cdsStats.setAd_campaign_key(keys.f1);
                cdsStats.setStt(DateTimeUtil.toHashDate(keys.f2));
                cdsStats.setEdt(DateTimeUtil.toHashDate(keys.f2));
                cdsStats.setTs(date.getTime());
                cdsStats.setHashTime(keys.f2);
                // cdsStats.setHashTime(keys.f2);
                out.collect(cdsStats);
            }
        }
    }


    public static class CdsStatsByAdProcessWindowFunction extends ProcessWindowFunction<CdsStatsDto, CdsStatsDto, Tuple3<String, String, Long>, TimeWindow> {

        @Override
        public void process(Tuple3<String, String, Long> keys, Context ctx, Iterable<CdsStatsDto> elements, Collector<CdsStatsDto> out) throws Exception {

            for (CdsStatsDto cdsStats : elements) {
                cdsStats.setChannel(keys.f0);
                cdsStats.setAd_campaign_key(keys.f1);
                cdsStats.setRegDate(keys.f2);
                cdsStats.setStt(ctx.window().getStart());
                cdsStats.setEdt(ctx.window().getEnd());
                cdsStats.setTs(new Date().getTime());
                out.collect(cdsStats);
            }
        }
    }

    public static class CdsStatsByAdDetailProcessWindowFunction extends ProcessWindowFunction<CdsStatsDto, CdsStatsDto, Tuple4<String, String, Long, Long>, TimeWindow> {

        @Override
        public void process(Tuple4<String, String, Long, Long> keys, Context ctx, Iterable<CdsStatsDto> elements, Collector<CdsStatsDto> out) throws Exception {

            for (CdsStatsDto cdsStats : elements) {
                cdsStats.setChannel(keys.f0);
                cdsStats.setAd_campaign_key(keys.f1);
                cdsStats.setRegDate(keys.f2);
                cdsStats.setStt(ctx.window().getStart());
                cdsStats.setEdt(ctx.window().getEnd());
                cdsStats.setTs(new Date().getTime());
                cdsStats.setDynamicDimension(keys.f3);
                out.collect(cdsStats);
            }
        }
    }


}
