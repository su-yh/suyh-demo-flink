package com.leomaster.batch.related;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.batch.func.DimMapFunction;
import com.leomaster.batch.func.UserRegDateDimMapFunction;
import com.leomaster.dto.TbRechargeDto;
import com.leomaster.dto.TbUserDto;
import com.leomaster.dto.TbUserLoginDto;
import com.leomaster.dto.TbWithdrawalDto;
import com.leomaster.utils.DateTimeUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BatchEventsWideTable {

    private final static Logger LOGGER = LoggerFactory.getLogger(BatchEventsWideTable.class);

    public static void setup(Map<String, DataSet<String>> linkedDSMap, Map<String, Object> configProperties) {
        final String tbUserSinkQueue = "dwm_tb_user";
        final String tbUserLoginSinkQueue = "dwm_tb_user_login";
        final String tbRechargeSinkQueue = "dwm_tb_recharge";
        final String tbWithdrawalSinkQueue = "dwm_tb_withdrawal";

        // tb_user 数据处理
        final DataSet<String> tbUserSS = linkedDSMap.get("dwd_tb_user");
        if (tbUserSS != null) {
            //tb_user数据源进行映射：解析为POJO对象、关联推广adkey、抽取时间戳和watermarks信息
            DataSet<TbUserDto> tbUserWithAdDS = tbUserSS.map((MapFunction<String, TbUserDto>) jsonString -> {
                TbUserDto tbUser = JSON.parseObject(jsonString, TbUserDto.class);
                //user表中的时间肯定是用户的注册时间，所以直接对后面需要统计的维度字段进行赋值即可
                tbUser.setRegDate(Long.valueOf(tbUser.getDay()));
                tbUser.setStatRegDate(Long.valueOf(tbUser.getDay()));
                String hash_time = DateTimeUtil.toHashTime(tbUser.getCtime());
                tbUser.setHashTime(hash_time);
                tbUser.setChannel(tbUser.getChannel().trim()); //防止channel数据有可能带空格的问题
                return tbUser;
            }).map(new UserRegDateDimMapFunction<TbUserDto>(configProperties) {
                @Override
                public Tuple4<String, String, String, String> getCondition(TbUserDto obj) {
                    return Tuple4.of(obj.getUid(), obj.getChannel(), obj.getDay(), obj.getPn());
                }

                @Override
                public void join(TbUserDto obj, JSONObject dimInfoJsonObj) throws Exception {
                    try {

                        if (dimInfoJsonObj == null) {
                            LOGGER.info("TbUserLogin join reg date failed, user uid = {}, ctime = {} , channel = {}",
                                    obj.getUid(), obj.getCtime(), obj.getChannel());

                            //代码应该到不了这里
                            obj.setStatRegDate(-1L);
                            obj.setNewRechargeDate(0L);
                        } else {
                            int regDate = dimInfoJsonObj.getLong("day") == null ? Integer.valueOf(obj.getDay()) : dimInfoJsonObj.getLong("day").intValue();
                            long rechargeCtime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                            long regDateTime = DateTimeUtil.transEventTimestamp(dimInfoJsonObj.getLong("ctime"), obj.getPn());
                            Long diffDay = DateTimeUtil.queryTimeDay(regDateTime, rechargeCtime);
                            obj.setDynamicDimension(diffDay);
                            long newRechargeDate = dimInfoJsonObj.getLong("newRechargeDate") == null ? 0L : dimInfoJsonObj.getLong("newRechargeDate");
                            obj.setNewRechargeDate(newRechargeDate);
                            obj.setStatRegDate(regDate);
                        }
                    } catch (Exception e) {
                        LOGGER.info("tbUserUserRegDate join exception:{},json:{}", obj, dimInfoJsonObj);
                        e.printStackTrace();
                    }
                }
            }).map(new DimMapFunction<TbUserDto>("adjust_user", configProperties) {
                @Override
                public Tuple2<String, String>[] getCondition(TbUserDto obj) {
                    return new Tuple2[]{Tuple2.of("gaid", obj.getGaid()),
                            Tuple2.of("channelid", obj.getChannel())
                    };
                }

                @Override
                public void join(TbUserDto obj, JSONObject dimInfoJsonObj) throws Exception {
                    String adCampaignKey = dimInfoJsonObj.getString("key");
                    LOGGER.info("TbUser join ad key, adCampaignKey = {}, user gaid {}, user uid = {}, channel = {}",
                            adCampaignKey, obj.getGaid(), obj.getUid(), obj.getChannel());
                    //处理key，还需要加上广告素材的相关信息
                    obj.setAdCampaignKey(adCampaignKey);
                    obj.setDynamicDimension(0L);
                }

                @Override
                public void dimFailed(TbUserDto obj) {
                    //TODO 对于adjust_user数据来的比较晚的情况，没有匹配到推广素材的数据，需要考虑放入侧输出流中延后再处理
                    LOGGER.info("TbUser join ad key failed, set ad key as Organic"
                            + ", user gaid = " + obj.getGaid()
                            + ", user uid = " + obj.getUid()
                            + ", ctime = " + obj.getCtime()
                            + ", channel = " + obj.getChannel());

                    String adCampaignKey = "Organic";
                    obj.setAdCampaignKey(adCampaignKey);
                }
            });
            linkedDSMap.put(tbUserSinkQueue, tbUserWithAdDS.map(tbUser -> JSON.toJSONString(tbUser)));
        }

        // tb_user_login 数据处理
        final DataSet<String> tbUserLoginSS = linkedDSMap.get("dwd_tb_user_login");
        if (tbUserLoginSS != null) {

            //tb_user_login数据源进行映射：解析为POJO对象、关联推广adkey、抽取时间戳和watermarks信息
            DataSet<TbUserLoginDto> tbUserLoginWithAdDS = tbUserLoginSS.map((MapFunction<String, TbUserLoginDto>) jsonString -> {
                TbUserLoginDto tbUserLogin = JSON.parseObject(jsonString, TbUserLoginDto.class);
                //for easy keyby later
                tbUserLogin.setLoginDate(Long.valueOf(tbUserLogin.getDay()));
                tbUserLogin.setHashTime(DateTimeUtil.toHashTime(tbUserLogin.getCtime()));
                tbUserLogin.setChannel(tbUserLogin.getChannel().trim()); //防止channel数据有可能带空格的问题
                return tbUserLogin;
            }).map(new UserRegDateDimMapFunction<TbUserLoginDto>(configProperties) {
                @Override
                public Tuple4<String, String, String, String> getCondition(TbUserLoginDto obj) {
                    return Tuple4.of(obj.getUid(), obj.getChannel(), obj.getDay(), obj.getPn());
                }

                @Override
                public void join(TbUserLoginDto obj, JSONObject dimInfoJsonObj) throws Exception {
                    try {

                        if (dimInfoJsonObj == null) {
                            LOGGER.info("TbUserLogin join reg date failed, user uid = {}, ctime = {} , channel = {}",
                                    obj.getUid(), obj.getCtime(), obj.getChannel());
                            long regDateTime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                            int regDate = DateTimeUtil.toIntDate(regDateTime);
                            obj.setStatRegDate(regDate);
                            //代码应该到不了这里
                            obj.setNewRechargeDate(0L);
                            obj.setUserRegChannel(""); //没有找到改数据，则无法获取用户的注册渠道
                        } else {

                            int regDate = dimInfoJsonObj.getLong("day") == null ? Integer.valueOf(obj.getDay()) : dimInfoJsonObj.getLong("day").intValue();

                            long rechargeCtime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                            long regDateTime = DateTimeUtil.transEventTimestamp(dimInfoJsonObj.getLong("ctime"), obj.getPn());

                            Long diffDay = DateTimeUtil.queryTimeDay(regDateTime, rechargeCtime);
                            obj.setDynamicDimension(diffDay);
                            long regDateTimeIgnoreChannel = dimInfoJsonObj.getLong("ctime_ig_ch");

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
                            } else {
                                obj.setUserRegChannel("");
                            }

                            obj.setStatRegDate(regDate);
                        }
                    } catch (Exception e) {
                        LOGGER.info("tbUserLoginWithRegDate join exception:{},json:{}", obj, dimInfoJsonObj);
                        e.printStackTrace();
                    }
                }
            }).map(
                    new DimMapFunction<TbUserLoginDto>("adjust_user", configProperties) {
                        @Override
                        public Tuple2<String, String>[] getCondition(TbUserLoginDto obj) {
                            return new Tuple2[]{Tuple2.of("gaid", obj.getGaid()),
                                    Tuple2.of("channelid", obj.getChannel())
                            };
                        }

                        @Override
                        public void join(TbUserLoginDto obj, JSONObject dimInfoJsonObj) throws Exception {
                            String adCampaignKey = dimInfoJsonObj.getString("key");
                            //处理key，还需要加上广告素材的相关信息
                            obj.setAdCampaignKey(adCampaignKey);
                        }

                        @Override
                        public void dimFailed(TbUserLoginDto obj) {
                            //TODO 对于adjust_user数据来的比较晚的情况，没有匹配到推广素材的数据，需要考虑放入侧输出流中延后再处理
                            String adCampaignKey = "Organic";
                            obj.setAdCampaignKey(adCampaignKey);
                        }
                    });
            linkedDSMap.put(tbUserLoginSinkQueue, tbUserLoginWithAdDS.map(tbUserLogin -> JSON.toJSONString(tbUserLogin)));
        }

        // tb_recharge 数据处理
        final DataSet<String> tbRechargeSourceStream = linkedDSMap.get("dwd_tb_recharge");
        if (tbRechargeSourceStream != null) {

            //tb_recharge数据源进行映射：解析为POJO对象、关联推广adkey、抽取时间戳和watermarks信息
            DataSet<TbRechargeDto> tbRechargeWithAdDS = tbRechargeSourceStream.map(new MapFunction<String, TbRechargeDto>() {
                @Override
                public TbRechargeDto map(String jsonString) throws Exception {
                    TbRechargeDto tbRecharge = JSON.parseObject(jsonString, TbRechargeDto.class);

                    long mtime = tbRecharge.getMtime() == 0 ? tbRecharge.getCtime() : tbRecharge.getMtime();
                    tbRecharge.setMtime(mtime);
                    tbRecharge.setRechargeDate(Long.valueOf(tbRecharge.getDay()));
                    String hash_time = DateTimeUtil.toHashTime(mtime);
                    tbRecharge.setHashTime(hash_time);
                    tbRecharge.setChannel(tbRecharge.getChannel().trim()); //防止channel数据有可能带空格的问题
                    return tbRecharge;
                }
            }).map(
                    new UserRegDateDimMapFunction<TbRechargeDto>(configProperties) {
                        @Override
                        public Tuple4<String, String, String, String> getCondition(TbRechargeDto obj) {
                            return Tuple4.of(obj.getUid(), obj.getChannel(), obj.getDay(), obj.getPn());
                        }

                        @Override
                        public void join(TbRechargeDto obj, JSONObject dimInfoJsonObj) throws Exception {
                            try {
                                if (dimInfoJsonObj == null) {
                                    LOGGER.info("TbRecharge join reg date failed, user uid = {}, ctime = {}, channel = {}",
                                            obj.getUid(), obj.getCtime(), obj.getChannel());
                                    //代码应该到不了这里
                                    long regDateTime = DateTimeUtil.transEventTimestamp(obj.getCtime(), obj.getPn());
                                    int regDate = DateTimeUtil.toIntDate(regDateTime);
                                    obj.setStatRegDate(regDate);
                                    obj.setNewRechargeDate(0L);
                                    obj.setStatRegDate(regDate);
                                    obj.setDynamicDimension(-1L);
                                } else {
                                    long rechargeCtime = DateTimeUtil.transEventTimestamp(obj.getMtime(), obj.getPn());
                                    long regDateTime = DateTimeUtil.transEventTimestamp(dimInfoJsonObj.getLong("ctime"), obj.getPn());

                                    int regDate = dimInfoJsonObj.getLong("day") == null ? DateTimeUtil.toIntDate(regDateTime) : dimInfoJsonObj.getLong("day").intValue();

                                    Long diffDay = DateTimeUtil.queryTimeDay(regDateTime, rechargeCtime);

                                    obj.setDynamicDimension(diffDay);

                                    long regDateTimeIgnoreChannel = dimInfoJsonObj.getLong("ctime_ig_ch");

                                    long newRechargeDate = dimInfoJsonObj == null || dimInfoJsonObj.getLong("newRechargeDate") == null ? 0L : dimInfoJsonObj.getLong("newRechargeDate");
                                    obj.setNewRechargeDate(newRechargeDate);

                                    if ("tb_user".equals(dimInfoJsonObj.getString("source"))) {
                                        //只有注册时间是从user表中关联到的，才赋值用户真正的注册日期这个字段
                                        obj.setRegDate(regDate);

                                    } else if (regDateTimeIgnoreChannel != 0) {
                                        regDateTimeIgnoreChannel = DateTimeUtil.transEventTimestamp(regDateTimeIgnoreChannel, obj.getPn());
                                        int regDateIgnoreChannel = DateTimeUtil.toIntDate(regDateTimeIgnoreChannel);
                                        obj.setRegDate(regDateIgnoreChannel);
                                    }
                                    obj.setStatRegDate(regDate);
                                }
                            } catch (Exception e) {
                                LOGGER.info("tbUserRechargeWithRegDate join exception:{},json:{}", obj, dimInfoJsonObj);
                                e.printStackTrace();
                            }
                        }
                    }).map(
                    new DimMapFunction<TbRechargeDto>("adjust_user", configProperties) {
                        @Override
                        public Tuple2<String, String>[] getCondition(TbRechargeDto obj) {
                            return new Tuple2[]{Tuple2.of("gaid", obj.getGaid()),
                                    Tuple2.of("channelid", obj.getChannel())
                            };
                        }

                        @Override
                        public void join(TbRechargeDto obj, JSONObject dimInfoJsonObj) throws Exception {
                            String adCampaignKey = dimInfoJsonObj.getString("key");
                            //处理key，还需要加上广告素材的相关信息
                            obj.setAdCampaignKey(adCampaignKey);
                        }

                        @Override
                        public void dimFailed(TbRechargeDto obj) {
                            //TODO 对于adjust_user数据来的比较晚的情况，没有匹配到推广素材的数据，需要考虑放入侧输出流中延后再处理
                            String adCampaignKey = "Organic";
                            obj.setAdCampaignKey(adCampaignKey);
                        }
                    });

            linkedDSMap.put(tbRechargeSinkQueue, tbRechargeWithAdDS.map(tbRecharge -> JSON.toJSONString(tbRecharge)));
        }

        // tb_withdrawal 数据处理
        final DataSet<String> tbWithdrawalSourceStream = linkedDSMap.get("dwd_tb_withdrawal");
        if (tbWithdrawalSourceStream != null) {

            //tb_withdrawal数据源进行映射：解析为POJO对象、关联推广adkey、抽取时间戳和watermarks信息
            DataSet<TbWithdrawalDto> tbWithdrawalWithAdDS = tbWithdrawalSourceStream.map(new MapFunction<String, TbWithdrawalDto>() {
                @Override
                public TbWithdrawalDto map(String jsonString) throws Exception {
                    TbWithdrawalDto tbWithdrawal = JSON.parseObject(jsonString, TbWithdrawalDto.class);
                    tbWithdrawal.setMtime(tbWithdrawal.getMtime() == 0 ? tbWithdrawal.getCtime() : tbWithdrawal.getMtime());
                    tbWithdrawal.setHashTime(DateTimeUtil.toHashTime(tbWithdrawal.getMtime()));
                    tbWithdrawal.setWithdrawalDate(Long.valueOf(tbWithdrawal.getDay()));
                    tbWithdrawal.setChannel(tbWithdrawal.getChannel().trim()); //处理渠道数据可能包含空格的问题
                    return tbWithdrawal;
                }
            }).map(
                    new UserRegDateDimMapFunction<TbWithdrawalDto>(configProperties) {
                        @Override
                        public Tuple4<String, String, String, String> getCondition(TbWithdrawalDto obj) {
                            return Tuple4.of(obj.getUid(), obj.getChannel(), obj.getDay(), obj.getPn());
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
                                    obj.setDynamicDimension(-1L);
                                } else {
                                    long rechargeCtime = DateTimeUtil.transEventTimestamp(obj.getMtime(), obj.getPn());
                                    long regDateTime = DateTimeUtil.transEventTimestamp(dimInfoJsonObj.getLong("ctime"), obj.getPn());
//                                    int regDate = DateTimeUtil.toIntDate(regDateTime);
                                    int regDate = dimInfoJsonObj.getLong("day") == null ? DateTimeUtil.toIntDate(regDateTime) : dimInfoJsonObj.getLong("day").intValue();

                                    Long diffDay = DateTimeUtil.queryTimeDay(regDateTime, rechargeCtime);
                                    obj.setDynamicDimension(diffDay);
                                    long regDateTimeIgnoreChannel = dimInfoJsonObj.getLong("ctime_ig_ch");

                                    if ("tb_user".equals(dimInfoJsonObj.getString("source"))) {
                                        //只有注册时间是从user表中关联到的，才赋值用户真正的注册日期这个字段
                                        obj.setRegDate(regDate);
                                    } else if (regDateTimeIgnoreChannel != 0) {
                                        obj.setRegDate(regDate);
                                        LOGGER.info("TbWithdrawal join regDateIgnoreChannel = {}", regDate);
                                    }
                                    obj.setStatRegDate(regDate);
                                    long newRechargeDate = dimInfoJsonObj == null || dimInfoJsonObj.getLong("newRechargeDate") == null ? 0L : dimInfoJsonObj.getLong("newRechargeDate");
                                    obj.setNewRechargeDate(newRechargeDate);
                                }
                            } catch (Exception e) {
                                LOGGER.info("tbUserWithdrawalWithRegDate join exception:{},json:{}", obj, dimInfoJsonObj);
                                e.printStackTrace();
                            }
                        }
                    }).map(
                    new DimMapFunction<TbWithdrawalDto>("adjust_user", configProperties) {
                        @Override
                        public Tuple2<String, String>[] getCondition(TbWithdrawalDto obj) {
                            return new Tuple2[]{Tuple2.of("gaid", obj.getGaid()),
                                    Tuple2.of("channelid", obj.getChannel())
                            };
                        }

                        @Override
                        public void join(TbWithdrawalDto obj, JSONObject dimInfoJsonObj) throws Exception {
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
                            //TODO 对于adjust_user数据来的比较晚的情况，没有匹配到推广素材的数据，需要考虑放入侧输出流中延后再处理
                            LOGGER.info("TbWithdrawal join ad key failed, set ad key as Organic"
                                    + ", user gaid = " + obj.getGaid()
                                    + ", user uid = " + obj.getUid()
                                    + ", ctime = " + obj.getCtime()
                                    + ", channel = " + obj.getChannel());

                            String adCampaignKey = "Organic";
                            obj.setAdCampaignKey(adCampaignKey);
                        }

                    });
            linkedDSMap.put(tbWithdrawalSinkQueue, tbWithdrawalWithAdDS.map(tbWithdrawal -> JSON.toJSONString(tbWithdrawal)));
        }
    }
}
