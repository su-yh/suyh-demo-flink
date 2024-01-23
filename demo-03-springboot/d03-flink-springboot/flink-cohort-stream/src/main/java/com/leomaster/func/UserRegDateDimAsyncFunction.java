package com.leomaster.func;

import com.aiteer.springboot.core.context.FlinkSpringContext;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.utils.DateTimeUtil;
import com.leomaster.utils.MySQLUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Desc：异步关联用户注册时间多维度的函数
 * 匹配条件为uid + channel
 * 先从user表中关联，如果从user表中找到记录，则返回，
 * 如果user表中无记录，则需要从login、recharge、withdrawal表中找到时间最早的记录，并将该时间作为用户的注册时间
 */
public abstract class UserRegDateDimAsyncFunction<T> extends RichAsyncFunction<T, T> implements UserRegDateDimJoinFunction<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserRegDateDimAsyncFunction.class);

    //线程池对象的父接口生命（多态）
    private ExecutorService executorService;

    private final Map<String, Object> configProperties;
    private StringRedisTemplate redisTemplate;
    private HikariDataSource hikariDataSource;

    public UserRegDateDimAsyncFunction(Map<String, Object> configProperties) {
        this.configProperties = configProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        FlinkSpringContext.init(configProperties);

        this.hikariDataSource = FlinkSpringContext.getBean(HikariDataSource.class);
        this.redisTemplate = FlinkSpringContext.getBean(StringRedisTemplate.class);
        this.executorService = FlinkSpringContext.getBean("cohortThreadPool", ExecutorService.class);
    }

    @Override
    public void close() throws Exception {
        super.close();

        FlinkSpringContext.closeContext();
    }

    private JSONObject queryRegDateFromDb(String sql) {
        List<JSONObject> dimList = MySQLUtil.queryList(sql, JSONObject.class, true, hikariDataSource);
        if (!dimList.isEmpty()) {return dimList.get(0);}
        return null;
    }

    private JSONObject[] queryRegDateFromDb(List<String> sqls) {
        JSONObject[] results = new JSONObject[sqls.size()];
        List<List<JSONObject>> dimLists = MySQLUtil.queryLists(sqls, JSONObject.class, true, hikariDataSource);
        for (int i = 0; i < dimLists.size(); i++) {
            List<JSONObject> dimList = dimLists.get(i);
            if (dimList != null && !dimList.isEmpty()) {
                results[i] = dimList.get(0);
            } else {
                results[i] = null;
            }
        }
        return results;
    }

    private JSONObject getUserRegTime(String uid, String channel,String pn) {
        //查找顺序：redis->user->(login|recharge|withdrawal)
        final String redisKey = "dim:user_reg_date:" + uid + "_" + channel;

        String dimStr = redisTemplate.opsForValue().get(redisKey);

        JSONObject userRegTimeResult = null;
        JSONObject queryFromTbUserRechargeResult = null;
        if (dimStr != null) {
            //从redis中查到了数据，则转换结果后返回
            LOGGER.debug("从redis查询到用户的注册时间, redisKey = {}", redisKey);
            if (dimStr.isEmpty()) {
                // 数据库里面存在数据，但是是空值，则说明之前查过数据库，而数据库中不存在值，所以对应的数据为null。
                return null;
            }
            return JSON.parseObject(dimStr);
        } else {
            //查询模板
            final String sqlFormat1 = "select ctime, channel from %s where uid = '%s' and channel = '%s' order by ctime limit 1";
            final String sqlFormatPn = "select ctime, channel from %s where uid = '%s' and pn = '%s' order by ctime limit 1";
//            final String sqlFormat1 = "select ctime, channel from %s where uid = '%s' and ctime is not null order by ctime limit 1";
            final String sqlFormat2 = "select ctime, channel from %s where uid = '%s' and channel = '%s' order by ctime limit 1";

            final String sqlFormat3 = "select ctime, `day` , channel from %s where uid = '%s' and channel = '%s' order by `day`  limit 1";

            //如果redis中没有查询到数据，则需要按照规则依次从user、login、recharge、withdrawal表寻找用户最早出现的时间
            String selectFromTbUser = String.format(sqlFormat1, "tb_user", uid, channel);
            String selectFromTbUserPn = String.format(sqlFormatPn, "tb_user", uid, pn);

//            String selectFromTbUser = String.format(sqlFormat1, "tb_user", uid);  //从user表查询时忽略渠道
            String queryFromTbUserRecharge = String.format(sqlFormat3, "tb_recharge", uid,channel);  //从tb_recharge查询
            userRegTimeResult = queryRegDateFromDb(selectFromTbUser);
            queryFromTbUserRechargeResult = queryRegDateFromDb(queryFromTbUserRecharge);

            long queryFromTbUserRechargeResultCtime = 0L;
            long userRegTimeIgnoreChannel = 0L;
            if (userRegTimeResult != null) {

                if(queryFromTbUserRechargeResult!=null){
                    if (queryFromTbUserRechargeResult.containsKey("day") && queryFromTbUserRechargeResult.getLong("day") != null) {
                        queryFromTbUserRechargeResultCtime = queryFromTbUserRechargeResult.getLong("day");
                    }
                    queryFromTbUserRechargeResultCtime = DateTimeUtil.toIntDate(DateTimeUtil.transEventTimestamp(queryFromTbUserRechargeResult.getLong("ctime")));
                }
                userRegTimeResult.put("newRechargeDate",queryFromTbUserRechargeResultCtime);

                String userRegChannel = userRegTimeResult.getString("channel");
                if (!channel.equals(userRegChannel)) {
                    if (userRegTimeResult.containsKey("ctime") && userRegTimeResult.getLong("ctime") != null) {
                        userRegTimeIgnoreChannel = userRegTimeResult.getLong("ctime");
                    }

                    //user表中的渠道信息和查询条件不一致时，将查询结果置空，然后继续右面的逻辑，但是注册时间需要用来关联是否是新增用户，所以需要保留
                    userRegTimeResult = queryRegDateFromDb(selectFromTbUserPn);
                    if (userRegTimeResult != null) {
                        userRegTimeResult.put("newRechargeDate", queryFromTbUserRechargeResultCtime);
                    }
                } else {
                    userRegTimeResult = queryRegDateFromDb(selectFromTbUserPn);
                    if (userRegTimeResult != null) {
                        if (queryFromTbUserRechargeResult != null) {
                            if (queryFromTbUserRechargeResult.containsKey("day") && queryFromTbUserRechargeResult.getLong("day") != null) {
                                queryFromTbUserRechargeResultCtime = queryFromTbUserRechargeResult.getLong("day");
                            }
                        }
                        userRegTimeResult.put("newRechargeDate", queryFromTbUserRechargeResultCtime);
                    }
                }

            }


            if (userRegTimeResult != null) {
                LOGGER.debug("从user表查询到了用户的注册时间, uid = {}, channel = {}", uid, channel);
                //如果从tb_user表中查到了记录，则加上标记，直接返回
                userRegTimeResult.put("source", "tb_user");
            } else {
                long userRegTime = 0L;

                //因为不知道那张表的数据是最早出现的，所以只能三种数据都查出来再做一次比较，后续优化可以考虑维护一张针对用户最早出现时间的表，从而简化这里的查询逻辑
                final String selectFromTbUserLogin = String.format(sqlFormat2, "tb_user_login", uid, channel);
                final String selectFromTbRecharge = String.format(sqlFormat2, "tb_recharge", uid, channel);
                final String selectFromTbWithdrawal = String.format(sqlFormat2, "tb_withdrawal", uid, channel);


                String[] sqls = new String[]{selectFromTbUserLogin, selectFromTbRecharge, selectFromTbWithdrawal};
                JSONObject[] regDateResultFromTbEvent = queryRegDateFromDb(Arrays.asList(sqls));
                JSONObject regDateResultFromLogin = regDateResultFromTbEvent[0];
                JSONObject regDateResultFromRecharge = regDateResultFromTbEvent[1];
                JSONObject regDateResultFromWithdrawal = regDateResultFromTbEvent[2];

                if (regDateResultFromLogin != null) {
                    userRegTime = regDateResultFromLogin.getLong("ctime");
                    userRegTimeResult = regDateResultFromLogin;
                    userRegTimeResult.put("source", "tb_user_login");
                }
                if (regDateResultFromRecharge != null) {
                    final long earlyTimeFromRecharge = regDateResultFromRecharge.getLong("ctime");
                    if (earlyTimeFromRecharge < userRegTime) {
                        //如果从查出的时间更早，则需要更新用户在当前渠道出现的时间 
                        userRegTime = earlyTimeFromRecharge;
                        userRegTimeResult = regDateResultFromRecharge;
                        userRegTimeResult.put("source", "tb_recharge");
                    }
                }
                if (regDateResultFromWithdrawal != null) {
                    final long earlyTimeFromWithdrawal = regDateResultFromWithdrawal.getLong("ctime");
                    if (earlyTimeFromWithdrawal < userRegTime) {
                        //如果从查出的时间更早，则需要更新用户在当前渠道出现的时间
                        userRegTimeResult = regDateResultFromWithdrawal;
                        userRegTimeResult.put("source", "tb_withdrawal");
                    }
                }
                if(queryFromTbUserRechargeResult!=null){
                    if (queryFromTbUserRechargeResult.containsKey("day") && queryFromTbUserRechargeResult.getLong("day") != null) {
                        queryFromTbUserRechargeResultCtime = queryFromTbUserRechargeResult.getLong("day");
                    }
                    if(Objects.nonNull(userRegTimeResult)){
                        userRegTimeResult.put("newRechargeDate",queryFromTbUserRechargeResultCtime);
                    }
                }

            }
            //如果查到记录，则保存到redis，并返回
            if (userRegTimeResult != null) {
                //将忽略渠道的用户注册时间存到其他的字段
                userRegTimeResult.put("ctime_ig_ch", userRegTimeIgnoreChannel);

                LOGGER.debug("从数据库表查询到用户的注册时间, uid = {}, channel = {}, result = {}", uid, channel, userRegTimeResult.toJSONString());
            } else {
                LOGGER.debug("维度数据没有找到, uid: {}, channel: {}", uid, channel);
            }

            if (userRegTimeResult != null) {
                redisTemplate.opsForValue().set(redisKey, userRegTimeResult.toJSONString(), 1, TimeUnit.DAYS);
            } else {
                // 数据库中不存在值也缓存redis 一分钟，不要让所有查询都打到数据库中，减小数据库的压力。
                redisTemplate.opsForValue().set(redisKey, "", 1, TimeUnit.MINUTES);
            }
        }

        return userRegTimeResult;
    }

    /**
     * 发送异步请求的方法
     *
     * @param obj          流中的事实数据
     * @param resultFuture 异步处理结束之后，返回结果
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture)  {
        LOGGER.debug("提交用户注册时间维度异步查询任务");
        executorService.submit(
                () -> {
                    try {
                        //发送异步请求
                        long start = System.currentTimeMillis();

                        //从流中事实数据获取key
                        Tuple3<String, String,String> condition = getCondition(obj);
                        String uid = condition.f0;
                        String channel = condition.f1;
                        String pn = condition.f2;
                        LOGGER.debug("执行用户注册时间维度异步查询, uid={}, channel= {}", uid, channel);
                        //根据维度的主键到维度表中进行查询
                        JSONObject userRegTimeResult = getUserRegTime(uid, channel, pn);
                        //维度关联  流中的事实数据和查询出来的维度数据进行关联

                        //确保返回结果中存在有效的结果，否则就将返回值置空即可
                        if (userRegTimeResult != null) {
                            if (!userRegTimeResult.containsKey("ctime")
                                    || userRegTimeResult.getLong("ctime") == null
                                    || userRegTimeResult.getLong("ctime") == 0) {
                                LOGGER.warn("用户注册时间维度异步查询异常, uid={}, channel={}, result={}", uid, channel, userRegTimeResult.toJSONString());
                                userRegTimeResult = null;
                            }
                        }
                        join(obj, userRegTimeResult);

                        //System.out.println("维度关联后的对象:" + obj);
                        long end = System.currentTimeMillis();
                        LOGGER.debug("用户注册时间维度异步查询耗时{}毫秒, uid={}, channel={}", (end - start), uid, channel);
                        //将关联后的数据数据继续向下传递
                        resultFuture.complete(Collections.singleton(obj));
                    } catch (Exception e) {
                        LOGGER.error("[Exception]",e);
                        throw new RuntimeException("用户注册时间维度异步查询失败");
                    }
                }
        );
    }
}
