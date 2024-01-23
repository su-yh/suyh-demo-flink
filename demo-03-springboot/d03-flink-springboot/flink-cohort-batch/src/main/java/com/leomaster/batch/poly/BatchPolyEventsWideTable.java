package com.leomaster.batch.poly;

import com.aiteer.springboot.batch.properties.BatchJobProperties;
import com.aiteer.springboot.core.context.FlinkSpringContext;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.batch.func.DimMapFunction;
import com.leomaster.batch.func.RMQSender;
import com.leomaster.core.constants.PN;
import com.leomaster.core.util.TextUtils;
import com.leomaster.dto.CdsStatsDto;
import com.leomaster.utils.DateTimeUtil;
import com.leomaster.utils.MyRMQUtil;
import com.leomaster.utils.mq.sink.MQ2ExSender;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class BatchPolyEventsWideTable {

    private final static Logger LOGGER = LoggerFactory.getLogger(BatchPolyEventsWideTable.class);


    public static void setup(int date,
                             Map<String, DataSet<String>> linkedDSMap, Map<String, Object> configProperties) {

        long dateTime = DateTimeUtil.toTs(String.valueOf(date), "yyyyMMdd");

        final String cdsStatsSinkQueue = "poly_stats_live";
        final String cdsDetailStatsSinkQueue = "poly_detail_stats_live";
        final String cdsChannelStatsSinkQueue = "poly_real_curve_live";

        final DataSet<String> tbUserDS = linkedDSMap.get("dwm_tb_user");
        final DataSet<String> tbUserLoginDS = linkedDSMap.get("dwm_tb_user_login");
        final DataSet<String> tbRechargeDS = linkedDSMap.get("dwm_tb_recharge");
        final DataSet<String> tbWithdrawalDS = linkedDSMap.get("dwm_tb_withdrawal");


        //TODO 对转换后的流进行合并
        DataSet<CdsStatsDto> unionDS = null;

        //TODO 对转换后的流进行合并
        DataSet<CdsStatsDto> unionRe = null;

        if (tbUserDS != null) {


            //tb_user jsonString -> CdsStats
            MapOperator<String, CdsStatsDto> tbUserStatsDS = tbUserDS.map(
                    (MapFunction<String, CdsStatsDto>) jsonStr -> {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String day = jsonObj.getString("day");
                        long dateTime1 = DateTimeUtil.toMilliTs(day);
                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                        if (adCampaignKey == null) {
                            LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                            adCampaignKey = "Organic";
                        }

                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.HY.Name;
                        }

                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);
                        String hashTime = jsonObj.getString("hashTime");
                        //每条tb_user数据可认为一个新增用户
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
                                .regDate(regDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .hashTime(hashTime)
                                .new_user_ct(1L)
                                .stt(dateTime1)
                                .edt(dateTime1)
                                .pn(pn)
                                .dynamicDimension(0L)
                                .ts(ts).build();
                        return cdsStats;
                    });

            if (unionDS != null) {
                unionDS = unionDS.union(tbUserStatsDS);
            } else {
                unionDS = tbUserStatsDS;
            }

            //tb_user jsonString -> CdsStats
            MapOperator<String, CdsStatsDto> tbUserStatsRe = tbUserDS.map(
                    (MapFunction<String, CdsStatsDto>) jsonStr -> {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String day = jsonObj.getString("day");
                        long dateTime12 = DateTimeUtil.toMilliTs(day);
                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long regDate = jsonObj.getLong("regDate"); //用户的注册日期

                        long newRechargeDate = jsonObj.getLong("newRechargeDate") == null ? 0L : jsonObj.getLong("newRechargeDate");

                        if (adCampaignKey == null) {
                            LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                            adCampaignKey = "Organic";
                        }

                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.HY.Name;
                        }

                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);
                        String hashTime = jsonObj.getString("hashTime");
                        //每条tb_user数据可认为一个新增用户
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
                                .regDate(newRechargeDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .hashTime(hashTime)
                                .new_user_ct(1L)
                                .stt(dateTime12)
                                .edt(dateTime12)
                                .pn(pn)
                                .dynamicDimension(0L)
                                .ts(ts).build();
                        return cdsStats;
                    });

            if (unionRe != null) {
                unionRe = unionRe.union(tbUserStatsRe);
            } else {
                unionRe = tbUserStatsRe;
            }
        }

        if (tbUserLoginDS != null) {
            //tb_user_login jsonString -> CdsStats
            DataSet<CdsStatsDto> tbUserLoginStatsDS = tbUserLoginDS.map(
                    (MapFunction<String, CdsStatsDto>) jsonStr -> {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.HY.Name;
                        }
                        String day = jsonObj.getString("day");
                        long dateTime13 = DateTimeUtil.toMilliTs(day);
                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);
                        // 即 新增 = regDate，行为日期 = ts
//                        long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                        long regDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                        long eventDate = jsonObj.getLong("loginDate");
                        String userRegChannel = jsonObj.getString("userRegChannel");

                        long dynamicDimension = jsonObj.getLong("dynamicDimension") == null ? -1L : jsonObj.getLong("dynamicDimension"); //用于统计的充值的 动态24小时维度


                        if (adCampaignKey == null) {
                            LOGGER.warn("warning , adCampaignKey  is null, data = {}", jsonStr);
                            adCampaignKey = "Organic";
                        }

                        // TODO later 这里可以分流出一条包含用户注册时间的数据，从何可以利用注册时间获取到推广详情需要统计的维度

                        //次留，只有渠道号未变化并且注册日期为登录日期的前一天时，才计算为次留
                        int retentionUserCountInc = (channel.equals(userRegChannel) && (DateTimeUtil.deltaDays(eventDate, regDate)) == 1) ? 1 : 0;
                        int xRetentionUserCountInc = channel.equals(userRegChannel) ? 1 : 0;


                        // TODO later 这里可以分流出一条包含用户注册时间的数据，从何可以利用注册时间获取到推广详情需要统计的维度
                        long newRechargeDate = jsonObj.getLong("newRechargeDate") == null ? 0L : jsonObj.getLong("newRechargeDate");
                        //新增付费用户次留，只有渠道号未变化 并且 充值日期 为登录日期的前一天时，才计算为次留
                        long newUserRechargeCountIncDayretention = 0L;
                        if (newRechargeDate != 0L) {
                            newUserRechargeCountIncDayretention = 1L;
                        }


                        //每条tb_user_login数据可认为一个活跃用户数据
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
//                                .regDate(statRegDate)
                                .regDate(regDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .active_user_ct(1L)
                                .retention_user_ct(retentionUserCountInc)
                                .x_retention_user_ct(xRetentionUserCountInc)
                                .stt(dateTime13)
                                .edt(dateTime13)
                                .pn(pn)
                                .new_user_recharge_ct_dayretention(newUserRechargeCountIncDayretention)
                                .ts(ts)
                                .dynamicDimension(dynamicDimension)
                                .build();
                        //向下游输出
                        return cdsStats;
                    });
            if (unionDS != null) {
                unionDS = unionDS.union(tbUserLoginStatsDS);
            } else {
                unionDS = tbUserLoginStatsDS;
            }


            //tb_user_login jsonString -> CdsStats
            DataSet<CdsStatsDto> tbUserLoginStatsRe = tbUserLoginDS.map(
                    (MapFunction<String, CdsStatsDto>) jsonStr -> {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String pn = jsonObj.getString("pn");
                        if (TextUtils.isEmpty(pn)) {
                            pn = PN.HY.Name;
                        }
                        String day = jsonObj.getString("day");
                        long dateTime14 = DateTimeUtil.toMilliTs(day);
                        String channel = jsonObj.getString("channel");
                        String adCampaignKey = jsonObj.getString("adCampaignKey");
                        Long ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("ctime"), pn);
                        // 即 新增 = regDate，行为日期 = ts
//                        long regDate = jsonObj.getLong("regDate"); //用户的注册日期
                        long regDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                        long eventDate = jsonObj.getLong("loginDate");
                        String userRegChannel = jsonObj.getString("userRegChannel");

                        long dynamicDimension = jsonObj.getLong("dynamicDimension") == null ? -1L : jsonObj.getLong("dynamicDimension"); //用于统计的充值的 动态24小时维度


                        if (adCampaignKey == null) {
                            LOGGER.warn("warning , adCampaignKey  is null, data = {}", jsonStr);
                            adCampaignKey = "Organic";
                        }

                        // TODO later 这里可以分流出一条包含用户注册时间的数据，从何可以利用注册时间获取到推广详情需要统计的维度

                        //次留，只有渠道号未变化并且注册日期为登录日期的前一天时，才计算为次留
                        int retentionUserCountInc = (channel.equals(userRegChannel) && (DateTimeUtil.deltaDays(eventDate, regDate)) == 1) ? 1 : 0;
                        int xRetentionUserCountInc = channel.equals(userRegChannel) ? 1 : 0;


                        // TODO later 这里可以分流出一条包含用户注册时间的数据，从何可以利用注册时间获取到推广详情需要统计的维度
                        long newRechargeDate = jsonObj.getLong("newRechargeDate") == null ? 0L : jsonObj.getLong("newRechargeDate");
                        //新增付费用户次留，只有渠道号未变化 并且 充值日期 为登录日期的前一天时，才计算为次留
                        long newUserRechargeCountIncDayretention = 0L;

                        if (newRechargeDate != 0L) {
                            newUserRechargeCountIncDayretention = 1L;
                        }

                        //每条tb_user_login数据可认为一个活跃用户数据
                        CdsStatsDto cdsStats = CdsStatsDto.builder()
//                                .regDate(statRegDate)
                                .regDate(newRechargeDate)
                                .channel(channel)
                                .ad_campaign_key(adCampaignKey)
                                .active_user_ct(1L)
                                .retention_user_ct(retentionUserCountInc)
                                .x_retention_user_ct(xRetentionUserCountInc)
                                .stt(dateTime14)
                                .edt(dateTime14)
                                .pn(pn)
                                .new_user_recharge_ct_dayretention(newUserRechargeCountIncDayretention)
                                .ts(ts)
                                .dynamicDimension(dynamicDimension)
                                .build();
                        //向下游输出
                        return cdsStats;
                    });
            if (unionRe != null) {
                unionRe = unionRe.union(tbUserLoginStatsRe);
            } else {
                unionRe = tbUserLoginStatsRe;
            }
        }


        if (tbRechargeDS != null) {

            /**
             *  1、充值人数
             *  2、充值总金额
             *  3、新增充值人数
             *  4、新增充值总金额
             *  5、老用户充值人数
             *  6、老用户充值金额
             */
            DataSet<CdsStatsDto> tbRechargeStatsDS = tbRechargeDS
                    .map((MapFunction<String, JSONObject>) jsonStr -> {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        return jsonObj;
                    })
                    .groupBy(new KeySelector<JSONObject, String>() {
                        @Override
                        public String getKey(JSONObject jsonObject) throws Exception {
                            return jsonObject.getString("uid") + "_" + jsonObject.getString("channel") + "_" + jsonObject.getString("adCampaignKey") + "_" + jsonObject.getString("day") + "_" + (StringUtils.isBlank(jsonObject.getString("regDate")) ? "" : jsonObject.getString("regDate")) + "_" + (jsonObject.getLong("dynamicDimension") == null ? "" : jsonObject.getLong("dynamicDimension").toString());
//                        return jsonObject.getString("uid") + "_" + jsonObject.getString("channel") + "_" + jsonObject.getString("day");
                        }
                    })
                    .reduceGroup(new RichGroupReduceFunction<JSONObject, CdsStatsDto>() {
                        @Override
                        public void reduce(Iterable<JSONObject> iterable, Collector<CdsStatsDto> collector) throws Exception {
                            String uid = "";
                            String channel = "";
                            String adCampaignKey = "";

                            long statRegDate = 0;

                            long rechargeCountInc = 0;
                            double rechargeAmtInc = 0;
                            long newUserRechargeCountInc = 0;
                            double newUserRechargeAmtInc = 0;
                            long oldUserRechargeCountInc = 0;
                            double oldUserRechargeAmtInc = 0;
                            long newUserRechargeCountIncDayretention = 0;
                            long dynamicTimeRecharge = 0;
                            long dynamicDimension = -1L;
                            long ts = 0;
                            String pn = "";
                            String hashTime = null;
                            long dateTime = 0;
                            for (JSONObject jsonObj : iterable) {
                                pn = jsonObj.getString("pn");
                                if (TextUtils.isEmpty(pn)) {
                                    pn = PN.HY.Name;
                                }

                                uid = jsonObj.getString("uid"); //用户uid
                                channel = jsonObj.getString("channel");
                                adCampaignKey = jsonObj.getString("adCampaignKey");
                                ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);
                                dynamicTimeRecharge = jsonObj.getLong("dynamicTimeRecharge") == null ? 0 : jsonObj.getLong("dynamicTimeRecharge");
                                hashTime = jsonObj.getString("hashTime");
                                if (adCampaignKey == null) {
                                    LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                                    adCampaignKey = "Organic";
                                }

//                            statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                                String day = jsonObj.getString("day");
                                dateTime = DateTimeUtil.toMilliTs(day);
                                long regDate = jsonObj.getLong("statRegDate"); //用户的注册日期
                                statRegDate = regDate; //统计用的用户注册日期
                                long rechargeDate = jsonObj.getLong("rechargeDate"); //充值发生的日期
                                double goodsAmt = jsonObj.getDouble("goodsAmt"); //充值金额
                                dynamicDimension = jsonObj.getLong("dynamicDimension") == null ? -1L : jsonObj.getLong("dynamicDimension"); //用于统计的充值的 动态24小时维度
                                //如果多天
//                            String recordKey = uid + "_" + channel + "_" + rechargeDate;

                                //这里在计算新增用户充值的时候，还是严格按照user表中能找到注册时间的规则来计算
                                rechargeCountInc = 1L;
                                rechargeAmtInc += goodsAmt;
                                newUserRechargeCountInc = regDate == rechargeDate ? 1L : 0L;
                                newUserRechargeAmtInc += regDate == rechargeDate ? goodsAmt : 0L;
                                oldUserRechargeCountInc += regDate == rechargeDate ? 0L : 1L;
                                oldUserRechargeAmtInc += regDate == rechargeDate ? 0L : goodsAmt;

                            }

                            if (TextUtils.isEmpty(pn)) {
                                pn = PN.HY.Name;
                            }

                            //已经groupby， 最终只需要输出一条包含充值统计信息的记录
                            CdsStatsDto cdsStats = CdsStatsDto.builder()
                                    .uid(uid)
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
//                                .new_user_recharge_ct_dayretention(newUserRechargeCountIncDayretention)
                                    .dynamicDimension(dynamicDimension)
                                    .hashTime(hashTime)
                                    .stt(dateTime)
                                    .edt(dateTime)
                                    .pn(pn)
                                    .build();
                            //像下游算子输出
                            collector.collect(cdsStats);
                        }
                    });

            if (unionDS != null) {
                unionDS = unionDS.union(tbRechargeStatsDS);
            } else {
                unionDS = tbRechargeStatsDS;
            }


            /**
             *  1、充值人数
             *  2、充值总金额
             *  3、新增充值人数
             *  4、新增充值总金额
             *  5、老用户充值人数
             *  6、老用户充值金额
             */
            DataSet<CdsStatsDto> tbRechargeStatsRe = tbRechargeDS
                    .map((MapFunction<String, JSONObject>) jsonStr -> {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        return jsonObj;
                    })
                    .groupBy(new KeySelector<JSONObject, String>() {
                        @Override
                        public String getKey(JSONObject jsonObject) throws Exception {
                            return jsonObject.getString("uid") + "_" + jsonObject.getString("channel") + "_" + jsonObject.getString("adCampaignKey") + "_" + jsonObject.getString("day") + "_" + (StringUtils.isBlank(jsonObject.getString("newRechargeDate")) ? "" : jsonObject.getString("newRechargeDate"));
//                        return jsonObject.getString("uid") + "_" + jsonObject.getString("channel") + "_" + jsonObject.getString("day");
                        }
                    })
                    .reduceGroup(new RichGroupReduceFunction<JSONObject, CdsStatsDto>() {
                        @Override
                        public void reduce(Iterable<JSONObject> iterable, Collector<CdsStatsDto> collector) throws Exception {
                            String uid = "";
                            String channel = "";
                            String adCampaignKey = "";

                            long statRegDate = 0;

                            long rechargeCountInc = 0;
                            double rechargeAmtInc = 0;
                            long newUserRechargeCountInc = 0;
                            double newUserRechargeAmtInc = 0;
                            long oldUserRechargeCountInc = 0;
                            double oldUserRechargeAmtInc = 0;
                            long newUserRechargeCountIncDayretention = 0;
                            long dynamicTimeRecharge = 0;
                            long dynamicDimension = -1L;
                            long ts = 0;
                            String pn = "";
                            String hashTime = null;
                            long dateTime = 0;
                            long newRechargeDate = -1L;


                            for (JSONObject jsonObj : iterable) {
                                pn = jsonObj.getString("pn");
                                if (TextUtils.isEmpty(pn)) {
                                    pn = PN.HY.Name;
                                }

                                uid = jsonObj.getString("uid"); //用户uid
                                channel = jsonObj.getString("channel");
                                adCampaignKey = jsonObj.getString("adCampaignKey");
                                ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);
                                dynamicTimeRecharge = jsonObj.getLong("dynamicTimeRecharge") == null ? 0 : jsonObj.getLong("dynamicTimeRecharge");
                                hashTime = jsonObj.getString("hashTime");
                                if (adCampaignKey == null) {
                                    LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                                    adCampaignKey = "Organic";
                                }
                                newRechargeDate = jsonObj.getLong("newRechargeDate") == null ? -1L : jsonObj.getLong("newRechargeDate");

//                            statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                                String day = jsonObj.getString("day");
                                dateTime = DateTimeUtil.toMilliTs(day);
                                long regDate = jsonObj.getLong("statRegDate"); //用户的注册日期
                                statRegDate = regDate; //统计用的用户注册日期
                                long rechargeDate = jsonObj.getLong("rechargeDate"); //充值发生的日期
                                double goodsAmt = jsonObj.getDouble("goodsAmt"); //充值金额
                                dynamicDimension = jsonObj.getLong("dynamicDimension") == null ? -1L : jsonObj.getLong("dynamicDimension"); //用于统计的充值的 动态24小时维度
                                //如果多天
//                            String recordKey = uid + "_" + channel + "_" + rechargeDate;

                                //这里在计算新增用户充值的时候，还是严格按照user表中能找到注册时间的规则来计算
                                rechargeCountInc = rechargeCountInc + 1L;
                                rechargeAmtInc += goodsAmt;
                                newUserRechargeCountInc = newRechargeDate == rechargeDate ? 1L : 0L;
                                newUserRechargeAmtInc += newRechargeDate == rechargeDate ? goodsAmt : 0L;
                                oldUserRechargeCountInc += newRechargeDate == rechargeDate ? 0L : 1L;
                                oldUserRechargeAmtInc += newRechargeDate == rechargeDate ? 0L : goodsAmt;

                            }

                            if (TextUtils.isEmpty(pn)) {
                                pn = PN.HY.Name;
                            }

                            //已经groupby， 最终只需要输出一条包含充值统计信息的记录
                            CdsStatsDto cdsStats = CdsStatsDto.builder()
                                    .uid(uid)
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
//                                .new_user_recharge_ct_dayretention(newUserRechargeCountIncDayretention)
                                    .dynamicDimension(dynamicDimension)
                                    .hashTime(hashTime)
                                    .stt(dateTime)
                                    .edt(dateTime)
                                    .pn(pn)
                                    .build();
                            //像下游算子输出
                            collector.collect(cdsStats);
                        }
                    });
            if (unionRe != null) {
                unionRe = unionRe.union(tbRechargeStatsRe);
            } else {
                unionRe = tbRechargeStatsRe;
            }

        }

        if (tbWithdrawalDS != null) {

            /**
             *  1、提现人数
             *  2、提现总金额
             */
            DataSet<CdsStatsDto> tbWithdrawalStatsDS = tbWithdrawalDS
                    .map((MapFunction<String, JSONObject>) jsonStr -> {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        return jsonObj;
                    })
                    .groupBy(new KeySelector<JSONObject, String>() {
                        @Override
                        public String getKey(JSONObject jsonObject) throws Exception {
//                        return jsonObject.getString("uid") + "_" + jsonObject.getString("channel") + "_" + jsonObject.getString("day") + "_" + jsonObject.getString("hashTime");
                            return jsonObject.getString("uid") + "_" + jsonObject.getString("channel") + "_" + jsonObject.getString("adCampaignKey") + "_" + jsonObject.getString("day") + "_" + (StringUtils.isBlank(jsonObject.getString("regDate")) ? "" : jsonObject.getString("regDate")) + "_" + (jsonObject.getLong("dynamicDimension") == null ? "" : jsonObject.getLong("dynamicDimension").toString());
                        }
                    })
                    .reduceGroup(new RichGroupReduceFunction<JSONObject, CdsStatsDto>() {
                        @Override
                        public void reduce(Iterable<JSONObject> iterable, Collector<CdsStatsDto> collector) throws Exception {
                            String uid = "";
                            String channel = "";
                            String adCampaignKey = "";

                            long statRegDate = 0;
                            long withdrawalUserCountInc = 0;
                            double withdrawalAmtInc = 0;
                            long ts = 0;
                            String pn = "";
                            long dateTime = 0;
                            long dynamicDimension = 0;
                            String hashTime = null;
                            for (JSONObject jsonObj : iterable) {

                                pn = jsonObj.getString("pn");
                                if (TextUtils.isEmpty(pn)) {
                                    pn = PN.HY.Name;
                                }
                                String day = jsonObj.getString("day");
                                dateTime = DateTimeUtil.toMilliTs(day);
                                uid = jsonObj.getString("uid"); //用户uid
                                channel = jsonObj.getString("channel");
                                adCampaignKey = jsonObj.getString("adCampaignKey");
                                ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);
                                hashTime = jsonObj.getString("hashTime");
                                if (adCampaignKey == null) {
                                    LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                                    adCampaignKey = "Organic";
                                }

//                            statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                                statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
//                            long regDate = jsonObj.getLong("regDate"); //用户的注册日期
//                            long withdrawalDate = jsonObj.getLong("withdrawalDate"); //提现发生的日期
                                double amount = jsonObj.getDouble("amount"); //提现金额
                                dynamicDimension = jsonObj.getLong("dynamicDimension") == null ? -1L : jsonObj.getLong("dynamicDimension"); //用于统计的充值的 动态24小时维度

                                withdrawalUserCountInc += 1L;
                                withdrawalAmtInc += amount;
                            }

                            if (TextUtils.isEmpty(pn)) {
                                pn = PN.HY.Name;
                            }

                            //输出一条包含充值统计信息的记录
                            CdsStatsDto cdsStats = CdsStatsDto.builder()
                                    .regDate(statRegDate)
                                    .channel(channel)
                                    .ad_campaign_key(adCampaignKey)
                                    .ts(ts)
                                    .withdrawal_user_ct(withdrawalUserCountInc)
                                    .withdrawal_amt(withdrawalAmtInc)
                                    .hashTime(hashTime)
                                    .dynamicDimension(dynamicDimension)
                                    .stt(dateTime)
                                    .edt(dateTime)
                                    .pn(pn)
                                    .uid(uid)
                                    .build();
                            //像下游算子输出
                            collector.collect(cdsStats);
                        }
                    });

            if (unionDS != null) {
                unionDS = unionDS.union(tbWithdrawalStatsDS);
            } else {
                unionDS = tbWithdrawalStatsDS;
            }

            /**
             *  1、提现人数
             *  2、提现总金额
             */
            DataSet<CdsStatsDto> tbWithdrawalStatsRe = tbWithdrawalDS
                    .map((MapFunction<String, JSONObject>) jsonStr -> {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        return jsonObj;
                    })
                    .groupBy(new KeySelector<JSONObject, String>() {
                        @Override
                        public String getKey(JSONObject jsonObject) throws Exception {
                            return jsonObject.getString("uid") + "_" + jsonObject.getString("channel") + "_" + jsonObject.getString("adCampaignKey") + "_" + jsonObject.getString("day") + "_" + (StringUtils.isBlank(jsonObject.getString("newRechargeDate")) ? "" : jsonObject.getString("newRechargeDate"));
//                        return jsonObject.getString("uid") + "_" + jsonObject.getString("channel") + "_" + jsonObject.getString("adCampaignKey") + "_" + jsonObject.getString("day") + "_" + (StringUtils.isBlank(jsonObject.getString("regDate")) ? "" : jsonObject.getString("regDate")) + "_" + (jsonObject.getLong("dynamicTimeRecharge") == null ? "" : jsonObject.getLong("dynamicTimeRecharge").toString());
                        }
                    })
                    .reduceGroup(new RichGroupReduceFunction<JSONObject, CdsStatsDto>() {
                        @Override
                        public void reduce(Iterable<JSONObject> iterable, Collector<CdsStatsDto> collector) throws Exception {
                            String uid = "";
                            String channel = "";
                            String adCampaignKey = "";

                            long statRegDate = 0;
                            long withdrawalUserCountInc = 0;
                            double withdrawalAmtInc = 0;
                            long ts = 0;
                            String pn = "";
                            long dateTime = 0;
                            long dynamicDimension = 0;
                            String hashTime = null;
                            long newRechargeDate = 1L;

                            for (JSONObject jsonObj : iterable) {

                                pn = jsonObj.getString("pn");
                                if (TextUtils.isEmpty(pn)) {
                                    pn = PN.HY.Name;
                                }
                                String day = jsonObj.getString("day");
                                dateTime = DateTimeUtil.toMilliTs(day);
                                uid = jsonObj.getString("uid"); //用户uid
                                channel = jsonObj.getString("channel");
                                adCampaignKey = jsonObj.getString("adCampaignKey");
                                ts = DateTimeUtil.transEventTimestamp(jsonObj.getLong("mtime"), pn);
                                hashTime = jsonObj.getString("hashTime");
                                if (adCampaignKey == null) {
                                    LOGGER.warn("warning, adCampaignKey is null, data = {}", jsonObj.toJSONString());
                                    adCampaignKey = "Organic";
                                }

//                            statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
                                statRegDate = jsonObj.getLong("statRegDate"); //统计用的用户注册日期
//                            long regDate = jsonObj.getLong("regDate"); //用户的注册日期
//                            long withdrawalDate = jsonObj.getLong("withdrawalDate"); //提现发生的日期
                                double amount = jsonObj.getDouble("amount"); //提现金额
                                dynamicDimension = jsonObj.getLong("dynamicDimension"); //用于统计的提现动态24小时维度

                                withdrawalUserCountInc += 1L;
                                withdrawalAmtInc += amount;
                                newRechargeDate = jsonObj.getLong("newRechargeDate") == null ? -1L : jsonObj.getLong("newRechargeDate");

                            }

                            if (TextUtils.isEmpty(pn)) {
                                pn = PN.HY.Name;
                            }

                            //输出一条包含充值统计信息的记录
                            CdsStatsDto cdsStats = CdsStatsDto.builder()
                                    .regDate(newRechargeDate)
                                    .channel(channel)
                                    .ad_campaign_key(adCampaignKey)
                                    .ts(ts)
                                    .withdrawal_user_ct(withdrawalUserCountInc)
                                    .withdrawal_amt(withdrawalAmtInc)
                                    .hashTime(hashTime)
                                    .dynamicDimension(dynamicDimension)
                                    .stt(dateTime)
                                    .edt(dateTime)
                                    .pn(pn)
                                    .uid(uid)
                                    .build();
                            //像下游算子输出
                            collector.collect(cdsStats);
                        }
                    });
            if (unionRe != null) {
                unionRe = unionRe.union(tbWithdrawalStatsRe);
            } else {
                unionRe = tbWithdrawalStatsRe;
            }
        }

        if (unionDS == null) {
            LOGGER.warn("unionDS is null.");
            return;
        }

        DataSet<CdsStatsDto> cdsStatsWithAdsWideDS = unionDS.groupBy(
                        // 分组
                        new KeySelector<CdsStatsDto, String>() {
                            @Override
                            public String getKey(CdsStatsDto cdsStats) throws Exception {
//                                return cdsStats.getChannel() + "_" + cdsStats.getAd_campaign_key() + "_" + cdsStats.getRegDate();
                                return cdsStats.getChannel() + "_" + cdsStats.getAd_campaign_key() + "_" + cdsStats.getRegDate() + "_" + (cdsStats.getDynamicDimension() == null ? "" : cdsStats.getDynamicDimension().toString());
                            }
                        })
                .reduce(
                        // 累加
                        new CdsStatsReduceFunction(date)
                ).map(
                        // 补充参数
                        new DimMapFunction<CdsStatsDto>("adjust_ad", configProperties) {
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

                                LOGGER.warn("warning, adjust_ad dim failed by ad, stats: {}", obj);
                            }
                        }
                );


        DataSet<CdsStatsDto> cdsStatsWithAdsDetailWideDS = unionRe.groupBy(
                        // 分组
                        new KeySelector<CdsStatsDto, String>() {
                            @Override
                            public String getKey(CdsStatsDto cdsStats) throws Exception {
                                return cdsStats.getChannel() + "_" + cdsStats.getAd_campaign_key() + "_" + cdsStats.getRegDate();
//                                return cdsStats.getChannel() + "_" + cdsStats.getAd_campaign_key() + "_" + cdsStats.getRegDate() + "_" + (cdsStats.getDynamicDimension() == null ? "" : cdsStats.getDynamicDimension().toString());
                            }
                        })
                .reduce(
                        // 累加
                        new CdsStatsReduceFunction(date)
                ).map(
                        // 补充参数
                        new DimMapFunction<CdsStatsDto>("adjust_ad", configProperties) {
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
                        }
                );

        // 付费留存
        cdsStatsWithAdsDetailWideDS.map(new RMQOutputMapFunction(cdsStatsSinkQueue, cdsStatsSinkQueue, configProperties))
                .printOnTaskManager("Replay-Cds-Stats-By-Ad-Wide");

        // 同期群
        cdsStatsWithAdsWideDS.map(new RMQOutputMapFunction(cdsDetailStatsSinkQueue, cdsDetailStatsSinkQueue, configProperties))
                .printOnTaskManager("Replay-Cds-Stats-By-Ad-Detail-Wide");
        // topic
        cdsStatsWithAdsWideDS.map(new MQ2OutputMapFunction(configProperties))
                .printOnTaskManager("Replay-Cds-Stats-By-Ad-Detail-Wide");
    }

    /**
     * 统计数据聚合的函数，把之前分类的值进行相加，三种分组方式可以共用该聚合函数
     */
    public static class CdsStatsReduceFunction implements ReduceFunction<CdsStatsDto> {

        long statsTs = 0L;

        CdsStatsReduceFunction(int date) {
            statsTs = DateTimeUtil.toTs(String.valueOf(date), "yyyyMMdd");
        }

        @Override
        public CdsStatsDto reduce(CdsStatsDto stats1, CdsStatsDto stats2) throws Exception {
//            stats1.setStt(statsTs);
//            stats1.setEdt(statsTs);
            stats1.setNew_user_ct(stats1.getNew_user_ct() + stats2.getNew_user_ct());
            stats1.setActive_user_ct(stats1.getActive_user_ct() + stats2.getActive_user_ct());
            stats1.setRetention_user_ct(stats1.getRetention_user_ct() + stats2.getRetention_user_ct());
            stats1.setX_retention_user_ct(stats1.getX_retention_user_ct() + stats2.getX_retention_user_ct());
            stats1.setRecharge_user_ct(stats1.getRecharge_user_ct() + stats2.getRecharge_user_ct());
            stats1.setRecharge_amt(stats1.getRecharge_amt() + stats2.getRecharge_amt());
            stats1.setNew_user_recharge_ct(stats1.getNew_user_recharge_ct() + stats2.getNew_user_recharge_ct());
            stats1.setNew_user_recharge_amt(stats1.getNew_user_recharge_amt() + stats2.getNew_user_recharge_amt());
            stats1.setNew_user_recharge_ct_dayretention(stats1.getNew_user_recharge_ct_dayretention() + stats2.getNew_user_recharge_ct_dayretention());
            stats1.setNew_user_recharge_amt_dayretention(stats1.getNew_user_recharge_amt_dayretention() + stats2.getNew_user_recharge_amt_dayretention());
            stats1.setOld_user_recharge_ct(stats1.getOld_user_recharge_ct() + stats2.getOld_user_recharge_ct());
            stats1.setOld_user_recharge_amt(stats1.getOld_user_recharge_amt() + stats2.getOld_user_recharge_amt());
            stats1.setWithdrawal_user_ct(stats1.getWithdrawal_user_ct() + stats2.getWithdrawal_user_ct());
            stats1.setWithdrawal_amt(stats1.getWithdrawal_amt() + stats2.getWithdrawal_amt());
            return stats1;
        }
    }

    public static class RMQOutputMapFunction extends RichMapFunction<CdsStatsDto, CdsStatsDto> {
        private final Map<String, Object> configProperties;
        private final String queueName;
        private final String routingKey;

        private RMQSender<String> rmqSender;

        public RMQOutputMapFunction(String queueName, String routingKey, Map<String, Object> configProperties) {
            this.queueName = queueName;
            this.routingKey = routingKey;
            this.configProperties = configProperties;
        }

        @Override
        public CdsStatsDto map(CdsStatsDto cdsStats) throws Exception {
            LOGGER.info("RMQOutputMapFunction map, sender::{}", rmqSender);
            if (rmqSender != null) {
                rmqSender.invoke(JSON.toJSONString(cdsStats));
            }
            return cdsStats;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            FlinkSpringContext.init(configProperties);
            BatchJobProperties jobProperties = FlinkSpringContext.getBean(BatchJobProperties.class);
            RMQConnectionConfig rmqConnectionConfig = MyRMQUtil.buildRmqConfig(jobProperties.getCdsRmq().getSink().getConnect(), null);
//            String exchange = jobProperties.getCdsRmq().getSink().getExchange();
            String exchange = "";   // TODO: suyh - 看了原来的代码，这里是固定值就是空字符串，直接写到队列中的。
            rmqSender = new RMQSender<>(rmqConnectionConfig, exchange, queueName, routingKey, new SimpleStringSchema());
            try {
                rmqSender.open();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws IOException {
            if (rmqSender != null) {
                rmqSender.close();
            }

            FlinkSpringContext.closeContext();
        }
    }

    public static class MQ2OutputMapFunction extends RichMapFunction<CdsStatsDto, CdsStatsDto> {
        private final Map<String, Object> configProperties;
        private MQ2ExSender<String> rmqSender;

        public MQ2OutputMapFunction(Map<String, Object> configProperties) {
            this.configProperties = configProperties;
        }

        @Override
        public CdsStatsDto map(CdsStatsDto cdsStats) throws Exception {
            if (rmqSender != null) {
                rmqSender.invoke(JSON.toJSONString(cdsStats));
            }
            return cdsStats;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            FlinkSpringContext.init(configProperties);
            BatchJobProperties jobProperties = FlinkSpringContext.getBean(BatchJobProperties.class);
            RMQConnectionConfig rmqConnectionConfig = MyRMQUtil.buildRmqConfig(jobProperties.getCdsRmq().getSink().getConnect(), null);
            String exchange = jobProperties.getCdsRmq().getSink().getExchange();
            String routingKeyCohort = jobProperties.getCdsRmq().getSink().getRoutingKeyCohort();

            rmqSender = new MQ2ExSender<>(rmqConnectionConfig, exchange, routingKeyCohort, new SimpleStringSchema());
            try {
                rmqSender.open();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws IOException {
            if (rmqSender != null) {
                rmqSender.close();
            }

            FlinkSpringContext.closeContext();
        }
    }
}
