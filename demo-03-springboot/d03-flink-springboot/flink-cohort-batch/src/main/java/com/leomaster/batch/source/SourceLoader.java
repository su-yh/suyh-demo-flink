package com.leomaster.batch.source;

import com.aiteer.springboot.core.context.FlinkSpringContext;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.core.util.TextUtils;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceLoader.class);

    public static Map<String, Collection<JSONObject>> loadDbDataByDate(
            int date, String channel, Map<String, Object> configProperties) {
        try {
            FlinkSpringContext.init(configProperties);

            HikariDataSource hikariDataSource = FlinkSpringContext.getBean(HikariDataSource.class);
            return loadSourceDataByDate(date, channel, hikariDataSource);
        } finally {
            FlinkSpringContext.closeContext();
        }
    }

    private static Map<String, Collection<JSONObject>> loadSourceDataByDate(int date, String channel, HikariDataSource hikariDataSource) {
        LOGGER.info("loadSourceDataByDate in  date = {}", date);

        Map<String, Collection<JSONObject>> result = new HashMap<>();

        String tbUserSql = "select * from tb_user where `day` = " + date;
        if (!TextUtils.isEmpty(channel)) {
            tbUserSql += " and channel = '" + channel + "'";
        }

        String tbUserLoginSql = "select * from tb_user_login where `day` = " + date;
        if (!TextUtils.isEmpty(channel)) {
            tbUserLoginSql += " and channel = '" + channel + "'";
        }

        String tbRechargeSql = "select * from tb_recharge where `day` = " + date;
        if (!TextUtils.isEmpty(channel)) {
            tbRechargeSql += " and channel = '" + channel + "'";
        }

        String tbWithdrawalSql = "select * from tb_withdrawal where `day` = " + date;
        if (!TextUtils.isEmpty(channel)) {
            tbWithdrawalSql += " and channel = '" + channel + "'";
        }

        List<JSONObject> tbUserLogins = MyBatchSQLUtil.queryList(tbUserLoginSql, JSONObject.class, false, hikariDataSource) == null ? new ArrayList<>() : MyBatchSQLUtil.queryList(tbUserLoginSql, JSONObject.class, false, hikariDataSource);
        List<JSONObject> tbUsers = MyBatchSQLUtil.queryList(tbUserSql, JSONObject.class, false, hikariDataSource) == null ? new ArrayList<>() : MyBatchSQLUtil.queryList(tbUserSql, JSONObject.class, false, hikariDataSource);
        List<JSONObject> tbRecharges = MyBatchSQLUtil.queryList(tbRechargeSql, JSONObject.class, false, hikariDataSource) == null ? new ArrayList<>() : MyBatchSQLUtil.queryList(tbRechargeSql, JSONObject.class, false, hikariDataSource);
        List<JSONObject> tbWithdrawals = MyBatchSQLUtil.queryList(tbWithdrawalSql, JSONObject.class, false, hikariDataSource) == null ? new ArrayList<>() : MyBatchSQLUtil.queryList(tbWithdrawalSql, JSONObject.class, false, hikariDataSource);

        tbUserLogins.addAll(tbUsers);

        result.put("replay_tb_user", tbUsers);
        result.put("replay_tb_user_login", tbUserLogins);
        result.put("replay_tb_recharge", tbRecharges);
        result.put("replay_tb_withdrawal", tbWithdrawals);

        LOGGER.info("loadSourceDataByDate, channel = {}, size = [{}, {}, {}, {}]", channel, tbUsers.size(),
                tbUserLogins.size(),
                tbRecharges.size(),
                tbWithdrawals.size());

        return result;
    }

}
