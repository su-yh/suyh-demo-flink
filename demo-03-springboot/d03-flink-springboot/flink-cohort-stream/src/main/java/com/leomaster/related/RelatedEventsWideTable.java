package com.leomaster.related;

import com.aiteer.springboot.common.constants.ConstantUtils;
import com.aiteer.springboot.common.vo.RmqConnectProperties;
import com.aiteer.springboot.core.context.FlinkSpringContext;
import com.aiteer.springboot.stream.properties.StreamJobProperties;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.dto.TbRechargeDto;
import com.leomaster.dto.TbUserDto;
import com.leomaster.dto.TbUserLoginDto;
import com.leomaster.dto.TbWithdrawalDto;
import com.leomaster.func.DimAsyncFunction;
import com.leomaster.func.UserRegDateDimAsyncFunction;
import com.leomaster.utils.DateTimeUtil;
import com.leomaster.utils.MyRMQUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 统计宽表数据计算
 * 主要 join adjust 广告维度数据
 */
public class RelatedEventsWideTable {

    private final static Logger LOGGER = LoggerFactory.getLogger(RelatedEventsWideTable.class);

    public static void setup(Map<String, Object> configProperties, Map<String, DataStream<String>> linkedDSMap) {
        final String tbUserSinkQueue = "related_tb_user";
        final String tbUserLoginSinkQueue = "related_tb_user_login";
        final String tbRechargeSinkQueue = "related_tb_recharge";
        final String tbWithdrawalSinkQueue = "related_tb_withdrawal";


        final DataStream<String> tbUserSourceStream = linkedDSMap.get("wash_tb_user");

        final DataStream<String> tbUserLoginSourceStream = linkedDSMap.get("wash_tb_user_login");

        final DataStream<String> tbRechargeSourceStream = linkedDSMap.get("wash_tb_recharge");

        final DataStream<String> tbWithdrawalSourceStream = linkedDSMap.get("wash_tb_withdrawal");

        // 因为login数据无法完全覆盖到user数据，而活跃=登录+新增，所以利用侧输出流来分流，对login流的数据进行一个补充
        OutputTag<String> userLoginPatchOT = new OutputTag<String>("user-login-patch") {
        };

        StreamJobProperties properties = FlinkSpringContext.getBean(StreamJobProperties.class);
        StreamJobProperties.CdsRmq cdsRmq = properties.getCdsRmq();
        StreamJobProperties.SourceRmq rmqSource = cdsRmq.getSource();
        RmqConnectProperties sourceConnect = rmqSource.getConnect();

        //tb_user数据源进行映射：解析为POJO对象、关联推广adkey、抽取时间戳和watermarks信息
        SingleOutputStreamOperator<TbUserDto> tbUserDataStream = tbUserSourceStream.map((MapFunction<String, TbUserDto>) jsonString -> {
                    TbUserDto tbUser = JSON.parseObject(jsonString, TbUserDto.class);
                    long regDate = DateTimeUtil.transEventTimestamp(tbUser.getCtime(), tbUser.getPn());
                    //user表中的时间肯定是用户的注册时间，所以直接对后面需要统计的维度字段进行赋值即可
                    tbUser.setRegDate(DateTimeUtil.toIntDate(regDate));
                    tbUser.setHashTime(DateTimeUtil.toHashTime(regDate));
                    tbUser.setStatRegDate(DateTimeUtil.toIntDate(regDate));
                    if (tbUser != null && StringUtils.isNotBlank(tbUser.getChannel())) {
                        tbUser.setChannel(tbUser.getChannel().trim()); //处理渠道数据可能包含空格的问题
                    } else {
                        tbUser.setChannel("");
                        LOGGER.info(">>>tbUser channel is null data:{}", tbUser);
                    }
                    return tbUser;
                }).name("[Related]-userReg004_map")
                .keyBy(TbUserDto::getUid)
                .process(new KeyedProcessFunction<String, TbUserDto, TbUserDto>() {
                    @Override
                    public void processElement(TbUserDto tbUser, Context context, Collector<TbUserDto> collector) {
                        JSONObject tbUserJsonObj = JSON.parseObject(JSON.toJSONString(tbUser));
                        tbUserJsonObj.put("from_user_patch", 1);
                        context.output(userLoginPatchOT, tbUserJsonObj.toJSONString()); //加入login patch tag
                        collector.collect(tbUser);
                    }
                }).name("[Related]-userReg005_process");

        //获取测输出流，sink到login队列
        DataStream<String> loginPatchDS = tbUserDataStream.getSideOutput(userLoginPatchOT);
        RMQSink<String> loginPatchSink = MyRMQUtil.buildRmqSink(sourceConnect, rmqSource.getQueueUserLogin());
        loginPatchDS.addSink(loginPatchSink).name("[Related]-userReg006_sink-output");

        //tb_user_login数据源进行映射：解析为POJO对象、关联推广adkey、抽取时间戳和watermarks信息
        DataStream<TbUserLoginDto> tbUserLoginDS = tbUserLoginSourceStream.map((MapFunction<String, TbUserLoginDto>) jsonString -> {
            TbUserLoginDto tbUserLogin = JSON.parseObject(jsonString, TbUserLoginDto.class);

            long loginTime = DateTimeUtil.transEventTimestamp(tbUserLogin.getCtime(), tbUserLogin.getPn());
            tbUserLogin.setHashTime(DateTimeUtil.toHashTime(loginTime));
            //for easy keyby later
            tbUserLogin.setLoginDate(DateTimeUtil.toIntDate(loginTime));
            if (tbUserLogin != null && StringUtils.isNotBlank(tbUserLogin.getChannel())) {
                tbUserLogin.setChannel(tbUserLogin.getChannel().trim()); //处理渠道数据可能包含空格的问题
            } else {
                tbUserLogin.setChannel("");
                LOGGER.info(">>>tbuserlogin channel is null data:{}", tbUserLogin);
            }
            return tbUserLogin;
        }).name("[Related]-userLogin004_map");

        //tb_recharge数据源进行映射：解析为POJO对象、关联推广adkey、抽取时间戳和watermarks信息
        DataStream<TbRechargeDto> tbRechargeDataDS = tbRechargeSourceStream.map((MapFunction<String, TbRechargeDto>) jsonString -> {
            TbRechargeDto tbRecharge = JSON.parseObject(jsonString, TbRechargeDto.class);
            tbRecharge.setMtime(tbRecharge.getMtime() == 0 ? tbRecharge.getCtime() : tbRecharge.getMtime());
            long rechargeDate = DateTimeUtil.transEventTimestamp(tbRecharge.getMtime(), tbRecharge.getPn());
//            tbRecharge.setRechargeDate(DateTimeUtil.toIntDate(rechargeDate));
//            tbRecharge.setRechargeDate(Long.valueOf(tbRecharge.getDay()));
            tbRecharge.setRechargeDate(DateTimeUtil.toIntDate(rechargeDate));
            tbRecharge.setHashTime(DateTimeUtil.toHashTime(rechargeDate));
            if (tbRecharge != null && StringUtils.isNotBlank(tbRecharge.getChannel())) {
                tbRecharge.setChannel(tbRecharge.getChannel().trim()); //处理渠道数据可能包含空格的问题
            } else {
                tbRecharge.setChannel("");
                LOGGER.info(">>>tbRecharge channel is null data:{}", tbRecharge);
            }
            return tbRecharge;
        }).name("[Related]-recharge004_map");

        //tb_withdrawal数据源进行映射：解析为POJO对象、关联推广adkey、抽取时间戳和watermarks信息
        DataStream<TbWithdrawalDto> tbWithdrawalDS = tbWithdrawalSourceStream.map((MapFunction<String, TbWithdrawalDto>) jsonString -> {
            TbWithdrawalDto tbWithdrawal = JSON.parseObject(jsonString, TbWithdrawalDto.class);
            tbWithdrawal.setMtime(tbWithdrawal.getMtime() == 0 ? tbWithdrawal.getCtime() : tbWithdrawal.getMtime());
            long orderFinishDate = DateTimeUtil.transEventTimestamp(tbWithdrawal.getMtime(), tbWithdrawal.getPn());
            tbWithdrawal.setHashTime(DateTimeUtil.toHashTime(orderFinishDate));
            tbWithdrawal.setWithdrawalDate(DateTimeUtil.toIntDate(orderFinishDate));
            if (tbWithdrawal != null && StringUtils.isNotBlank(tbWithdrawal.getChannel())) {
                tbWithdrawal.setChannel(tbWithdrawal.getChannel().trim()); //处理渠道数据可能包含空格的问题
            } else {
                tbWithdrawal.setChannel("");
                LOGGER.info(">>>tbWithdrawal channel is null data:{}", tbWithdrawal);
            }
            return tbWithdrawal;
        }).name("[Related]-withdrawal004_map");

        //TODO 异步关联cds事件与广告素材
        DataStream<TbUserDto> tbUserWithAdDS = AsyncDataStream
                .unorderedWait(tbUserDataStream,
                        new DimAsyncFunction<TbUserDto>("adjust_user", configProperties) {
                            @Override
                            public Tuple2<String, String>[] getCondition(TbUserDto obj) {
                                return new Tuple2[]{Tuple2.of("gaid", obj.getGaid()),
                                        Tuple2.of("channelid", obj.getChannel())
                                };
                            }

                            @Override
                            public void join(TbUserDto obj, JSONObject dimInfoJsonObj) throws Exception {
                                String adCampaignKey = dimInfoJsonObj.getString("key") == null ? "" : dimInfoJsonObj.getString("key");
                                LOGGER.info("TbUser join ad key, adCampaignKey = {}, user gaid {}, user uid = {}, channel = {}",
                                        adCampaignKey, obj.getGaid(), obj.getUid(), obj.getChannel());
                                //处理key，还需要加上广告素材的相关信息
                                obj.setAdCampaignKey(adCampaignKey);
                                obj.setDynamicDimension(0L);
                            }

                            @Override
                            public void dimFailed(TbUserDto obj) {
                                //
                                LOGGER.info("TbUser join ad key failed, set ad key as Organic"
                                        + ", user gaid = " + obj.getGaid()
                                        + ", user uid = " + obj.getUid()
                                        + ", ctime = " + obj.getCtime()
                                        + ", channel = " + obj.getChannel());

                                String adCampaignKey = "Organic";
                                obj.setAdCampaignKey(adCampaignKey);
                            }
                        }, ConstantUtils.DIM_TIMEOUT_SECONDS, TimeUnit.SECONDS, ConstantUtils.DIM_CAPACITY)
                .name("[Related]-userReg007_asyncDataStream-adjust_user");

        DataStream<TbUserLoginDto> tbUserLoginWithRegDate = AsyncDataStream
                .unorderedWait(tbUserLoginDS,
                        new UserRegDateDimAsyncFunction<TbUserLoginDto>(configProperties) {
                            @Override
                            public Tuple3<String, String, String> getCondition(TbUserLoginDto obj) {
                                return Tuple3.of(obj.getUid(), obj.getChannel() == null ? "" : obj.getChannel(), obj.getPn());
                            }

                            @Override
                            public void join(TbUserLoginDto obj, JSONObject dimInfoJsonObj) {
                                try {
                                    if (dimInfoJsonObj == null) {
                                        LOGGER.info("TbUserLogin join reg date failed, user uid = {}, ctime = {} , channel = {}",
                                                obj.getUid(), obj.getCtime(), obj.getChannel());

                                        //代码应该到不了这里
                                        long regDateTime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                                        int regDate = DateTimeUtil.toIntDate(regDateTime);
                                        obj.setStatRegDate(regDate);
                                        obj.setNewRechargeDate(0L);
                                        obj.setUserRegChannel(""); //没有找到改数据，则无法获取用户的注册渠道
                                    } else {
                                        LOGGER.info("TbUserLogin join reg date, user uid = {}, ctime = {} , channel = {}, regDate = {}",
                                                obj.getUid(), obj.getCtime(), obj.getChannel(), dimInfoJsonObj.toJSONString());

                                        long rechargeCtime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                                        long regDateTime = DateTimeUtil.transEventTimestamp(dimInfoJsonObj.getLong("ctime"), obj.getPn());
                                        int regDate = DateTimeUtil.toIntDate(regDateTime);
                                        Long diffDay = DateTimeUtil.queryTimeDay(regDateTime, rechargeCtime);
                                        obj.setDynamicDimension(diffDay);


                                        long regDateTimeIgnoreChannel = dimInfoJsonObj.getLong("ctime_ig_ch");

                                        //long newRechargeDate = DateTimeUtil.transEventTimestamp(dimInfoJsonObj.getLong("newRechargeDate"), obj.getPn());
                                        long newRechargeDate = dimInfoJsonObj.getLong("newRechargeDate") == null ? 0L : dimInfoJsonObj.getLong("newRechargeDate");

                                        obj.setNewRechargeDate(newRechargeDate);
                                        if ("tb_user".equals(dimInfoJsonObj.getString("source"))) {
                                            //只有注册时间是从user表中关联到的，才赋值用户真正的注册日期这个字段
                                            obj.setRegDate(regDate);
                                            //同样只有从user表查到记录，才设置注册渠道的信息，该字段用于后续留存数据的判断
                                            String regChannel = dimInfoJsonObj.getString("channel");
                                            obj.setUserRegChannel(regChannel);
                                        } else if (regDateTimeIgnoreChannel != 0) {
                                            regDateTimeIgnoreChannel = DateTimeUtil.transEventTimestamp(regDateTimeIgnoreChannel, obj.getPn());
                                            int regDateIgnoreChannel = DateTimeUtil.toIntDate(regDateTimeIgnoreChannel);
                                            obj.setRegDate(regDateIgnoreChannel);

                                            obj.setUserRegChannel(""); //注册渠道肯定与当前登录渠道不一致，置空即可
                                            LOGGER.debug("TbUserLogin join regDateIgnoreChannel = {}", regDateIgnoreChannel);
                                        } else {
                                            obj.setUserRegChannel("");
                                        }

                                        obj.setStatRegDate(regDate);
                                    }
                                } catch (Exception e) {
                                    LOGGER.error("[Exception]", e);
                                }
                            }
                        },
                        ConstantUtils.DIM_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS,
                        ConstantUtils.DIM_CAPACITY)
                .name("[Related]-userLogin005_async");

        //TODO login事件关联广告信息
        DataStream<TbUserLoginDto> tbUserLoginWithAdDS = AsyncDataStream
                .unorderedWait(tbUserLoginWithRegDate,
                        new DimAsyncFunction<TbUserLoginDto>("adjust_user", configProperties) {
                            @Override
                            public Tuple2<String, String>[] getCondition(TbUserLoginDto obj) {
                                return new Tuple2[]{Tuple2.of("gaid", obj.getGaid()),
                                        Tuple2.of("channelid", obj.getChannel())
                                };
                            }

                            @Override
                            public void join(TbUserLoginDto obj, JSONObject dimInfoJsonObj) throws Exception {
                                String adCampaignKey = dimInfoJsonObj.getString("key");
                                LOGGER.info("TbUserLogin join ad key, adCampaignKey = " + adCampaignKey
                                        + ", user gaid = " + obj.getGaid()
                                        + ", user uid = " + obj.getUid()
                                        + ", ctime = " + obj.getCtime()
                                        + ", channel = " + obj.getChannel());
                                //处理key，还需要加上广告素材的相关信息

                                obj.setAdCampaignKey(adCampaignKey);
                            }

                            @Override
                            public void dimFailed(TbUserLoginDto obj) {
                                //
                                LOGGER.info("TbUserLogin join ad key failed, set ad key as Organic"
                                        + ", user gaid = " + obj.getGaid()
                                        + ", user uid = " + obj.getUid()
                                        + ", ctime = " + obj.getCtime()
                                        + ", channel = " + obj.getChannel());

                                String adCampaignKey = "Organic";
                                obj.setAdCampaignKey(adCampaignKey);
                            }
                        }, ConstantUtils.DIM_TIMEOUT_SECONDS, TimeUnit.SECONDS, ConstantUtils.DIM_CAPACITY)
                .name("[Related]-userLogin006_async");


        //TODO 对recharge事件进行维度关联
        //TODO recharge事件关联用户注册时间
        DataStream<TbRechargeDto> tbUserRechargeWithRegDate = AsyncDataStream
                .unorderedWait(tbRechargeDataDS,
                        new UserRegDateDimAsyncFunction<TbRechargeDto>(configProperties) {
                            @Override
                            public Tuple3<String, String, String> getCondition(TbRechargeDto obj) {
                                return Tuple3.of(obj.getUid(), obj.getChannel() == null ? "" : obj.getChannel(), obj.getPn());
                            }

                            @Override
                            public void join(TbRechargeDto obj, JSONObject dimInfoJsonObj) {
                                try {
                                    if (dimInfoJsonObj == null) {
                                        LOGGER.debug("TbRecharge join reg date failed, user uid = {}, ctime = {}, channel = {}",
                                                obj.getUid(), obj.getCtime(), obj.getChannel());

                                        //代码应该到不了这里
                                        long regDateTime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                                        int regDate = DateTimeUtil.toIntDate(regDateTime);

                                        obj.setNewRechargeDate(0L);
                                        obj.setStatRegDate(regDate);
                                        obj.setDynamicDimension(0L);
                                    } else {
                                        LOGGER.info("TbRecharge join reg date, user uid = {}, ctime = {}, channel = {}, regDate = {}",
                                                obj.getUid(), obj.getCtime(), obj.getChannel(), dimInfoJsonObj.toJSONString());
                                        long rechargeCtime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                                        long regDateTime = DateTimeUtil.transEventTimestamp(dimInfoJsonObj.getLong("ctime"), obj.getPn());
                                        int regDate = DateTimeUtil.toIntDate(regDateTime);
                                        LOGGER.info("TbRecharge join reg date = {}", regDate);

                                        Long diffDay = DateTimeUtil.queryTimeDay(regDateTime, rechargeCtime);
                                        obj.setDynamicDimension(diffDay);

                                        long regDateTimeIgnoreChannel = dimInfoJsonObj.getLong("ctime_ig_ch");

                                        long newRechargeDate = dimInfoJsonObj.getLong("newRechargeDate") == null ? 0L : dimInfoJsonObj.getLong("newRechargeDate");

                                        obj.setNewRechargeDate(newRechargeDate);

                                        if ("tb_user".equals(dimInfoJsonObj.getString("source"))) {
                                            //只有注册时间是从user表中关联到的，才赋值用户真正的注册日期这个字段
                                            obj.setRegDate(regDate);

                                        } else if (regDateTimeIgnoreChannel != 0) {
                                            regDateTimeIgnoreChannel = DateTimeUtil.transEventTimestamp(regDateTimeIgnoreChannel, obj.getPn());
                                            int regDateIgnoreChannel = DateTimeUtil.toIntDate(regDateTimeIgnoreChannel);
                                            obj.setRegDate(regDateIgnoreChannel);
                                            LOGGER.info("TbRecharge join regDateIgnoreChannel = {}", regDateIgnoreChannel);
                                        }
                                        obj.setStatRegDate(regDate);
                                    }
                                } catch (Exception e) {
                                    LOGGER.error("[Exception]", e);
                                }
                            }
                        }, ConstantUtils.DIM_TIMEOUT_SECONDS, TimeUnit.SECONDS, ConstantUtils.DIM_CAPACITY)
                .name("[Related]-recharge005_async");

        //TODO recharge事件关联广告信息
        DataStream<TbRechargeDto> tbRechargeWithAdDS = AsyncDataStream
                .unorderedWait(tbUserRechargeWithRegDate,
                        new DimAsyncFunction<TbRechargeDto>("adjust_user", configProperties) {
                            @Override
                            public Tuple2<String, String>[] getCondition(TbRechargeDto obj) {
                                return new Tuple2[]{Tuple2.of("gaid", obj.getGaid()),
                                        Tuple2.of("channelid", obj.getChannel())
                                };
                            }

                            @Override
                            public void join(TbRechargeDto obj, JSONObject dimInfoJsonObj) throws Exception {
                                String adCampaignKey = dimInfoJsonObj.getString("key");
                                LOGGER.debug("TbRecharge join ad key, adCampaignKey = " + adCampaignKey
                                        + ", user gaid = " + obj.getGaid()
                                        + ", user uid = " + obj.getUid()
                                        + ", ctime = " + obj.getCtime()
                                        + ", channel = " + obj.getChannel());
                                //处理key，还需要加上广告素材的相关信息
                                obj.setAdCampaignKey(adCampaignKey);
                            }

                            @Override
                            public void dimFailed(TbRechargeDto obj) {
                                // 
                                LOGGER.info("TbRecharge join ad key failed, set ad key as Organic"
                                        + ", user gaid = " + obj.getGaid()
                                        + ", user uid = " + obj.getUid()
                                        + ", ctime = " + obj.getCtime()
                                        + ", channel = " + obj.getChannel());

                                String adCampaignKey = "Organic";
                                obj.setAdCampaignKey(adCampaignKey);
                            }
                        }, ConstantUtils.DIM_TIMEOUT_SECONDS, TimeUnit.SECONDS, ConstantUtils.DIM_CAPACITY)
                .name("[Related]-recharge006_async");

        //TODO 对withdrawal事件进行维度关联
        //TODO withdrawal事件关联用户注册时间
        DataStream<TbWithdrawalDto> tbUserWithdrawalWithRegDate = AsyncDataStream
                .unorderedWait(tbWithdrawalDS,
                        new UserRegDateDimAsyncFunction<TbWithdrawalDto>(configProperties) {
                            @Override
                            public Tuple3<String, String, String> getCondition(TbWithdrawalDto obj) {
                                return Tuple3.of(obj.getUid(), obj.getChannel() == null ? "" : obj.getChannel(), obj.getPn());
                            }

                            @Override
                            public void join(TbWithdrawalDto obj, JSONObject dimInfoJsonObj) throws Exception {
                                try {
                                    if (dimInfoJsonObj == null) {
                                        LOGGER.info("TbWithdrawal join reg date failed, user uid = {}, ctime = {}, channel = {}",
                                                obj.getUid(), obj.getCtime(), obj.getChannel());

                                        //代码应该到不了这里
                                        long regDateTime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                                        int regDate = DateTimeUtil.toIntDate(regDateTime);

                                        obj.setStatRegDate(regDate);
                                        obj.setDynamicDimension(0L);
                                    } else {
                                        LOGGER.info("TbWithdrawal join reg date, user uid = {}, ctime = {}, channel = {}, regDate = {}",
                                                obj.getUid(), obj.getCtime(), obj.getChannel(), dimInfoJsonObj.toJSONString());
//                                        long rechargeCtime = DateTimeUtil.transEventTimestamp(obj.getMtime(), obj.getPn());
                                        long rechargeCtime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                                        long regDateTime = DateTimeUtil.transEventTimestamp(dimInfoJsonObj.getLong("ctime"), obj.getPn());
                                        int regDate = DateTimeUtil.toIntDate(regDateTime);
                                        Long diffDay = DateTimeUtil.queryTimeDay(regDateTime, rechargeCtime);
                                        obj.setDynamicDimension(diffDay);
                                        long regDateTimeIgnoreChannel = dimInfoJsonObj.getLong("ctime_ig_ch");

                                        if ("tb_user".equals(dimInfoJsonObj.getString("source"))) {
                                            //只有注册时间是从user表中关联到的，才赋值用户真正的注册日期这个字段
                                            obj.setRegDate(regDate);
                                        } else if (regDateTimeIgnoreChannel != 0) {
                                            regDateTimeIgnoreChannel = DateTimeUtil.transEventTimestamp(regDateTimeIgnoreChannel, obj.getPn());
                                            int regDateIgnoreChannel = DateTimeUtil.toIntDate(regDateTimeIgnoreChannel);
                                            obj.setRegDate(regDateIgnoreChannel);
                                            LOGGER.info("TbWithdrawal join regDateIgnoreChannel = {}", regDateIgnoreChannel);
                                        }
                                        obj.setStatRegDate(regDate);
                                    }
                                } catch (Exception e) {
                                    LOGGER.error("[Exception]", e);
                                }
                            }
                        }, ConstantUtils.DIM_TIMEOUT_SECONDS, TimeUnit.SECONDS, ConstantUtils.DIM_CAPACITY)
                .name("[Related]-withdrawal005_map");

        //TODO withdrawal事件关联广告信息
        DataStream<TbWithdrawalDto> tbWithdrawalWithAdDS = AsyncDataStream
                .unorderedWait(tbUserWithdrawalWithRegDate,
                        new DimAsyncFunction<TbWithdrawalDto>("adjust_user", configProperties) {
                            @Override
                            public Tuple2<String, String>[] getCondition(TbWithdrawalDto obj) {
                                return new Tuple2[]{Tuple2.of("gaid", obj.getGaid()),
                                        Tuple2.of("channelid", obj.getChannel())
                                };
                            }

                            @Override
                            public void join(TbWithdrawalDto obj, JSONObject dimInfoJsonObj) {
                                String adCampaignKey = dimInfoJsonObj.getString("key");
                                LOGGER.info("TbWithdrawal join ad key, adCampaignKey = " + adCampaignKey
                                        + ", user gaid = " + obj.getGaid()
                                        + ", user uid = " + obj.getUid()
                                        + ", ctime = " + obj.getCtime()
                                        + ", channel = " + obj.getChannel());
                                //处理key，还需要加上广告素材的相关信息
                                obj.setAdCampaignKey(adCampaignKey);
                            }

                            @Override
                            public void dimFailed(TbWithdrawalDto obj) {
                                //
                                LOGGER.info("TbWithdrawal join ad key failed, set ad key as Organic"
                                        + ", user gaid = " + obj.getGaid()
                                        + ", user uid = " + obj.getUid()
                                        + ", ctime = " + obj.getCtime()
                                        + ", channel = " + obj.getChannel());

                                String adCampaignKey = "Organic";
                                obj.setAdCampaignKey(adCampaignKey);
                            }
                        }, ConstantUtils.DIM_TIMEOUT_SECONDS, TimeUnit.SECONDS, ConstantUtils.DIM_CAPACITY)
                .name("[Related]-withdrawal006_async");


        //TODO 不在dwm层做聚合，把做完维度关联的数据发回给mq，在dws层进行最后的聚合操作

        linkedDSMap.put(tbUserSinkQueue, tbUserWithAdDS.map(tbUser -> JSON.toJSONString(tbUser)));


        linkedDSMap.put(tbUserLoginSinkQueue, tbUserLoginWithAdDS.map(tbUserLogin -> JSON.toJSONString(tbUserLogin)));


        linkedDSMap.put(tbRechargeSinkQueue, tbRechargeWithAdDS.map(tbRecharge -> JSON.toJSONString(tbRecharge)));


        linkedDSMap.put(tbWithdrawalSinkQueue, tbWithdrawalWithAdDS.map(tbWithdrawal -> JSON.toJSONString(tbWithdrawal)));

    }

}
