package com.leomaster.dto;

import lombok.Builder;
import lombok.Data;


/**
 * @Builder注解 可以使用构造者方式创建对象，给属性赋值
 * @Builder.Default 在使用构造者方式给属性赋值的时候，属性的初始值会丢失
 * 该注解的作用就是修复这个问题
 * 例如：我们在属性上赋值了初始值为0L，如果不加这个注解，通过构造者创建的对象属性值会变为null
 */
@Data
@Builder
public class CdsStatsDto {

    private Long stt; //窗口起始时间

    private Long edt; //窗口结束时间

    @Builder.Default
    private long regDate = 0L; //用户注册日期

    @Builder.Default
    private long newRechargeDate =0L;

    @Builder.Default
    private long loginDate = 0L; //用户登录时间

    @Builder.Default
    private String channel = ""; //渠道号

    @Builder.Default
    private String ad_campaign_key = ""; //投放广告素材key

    @Builder.Default
    private Long new_user_ct = 0L; //新增用户数

    @Builder.Default
    private Integer retention_user_ct = 0; //留存用户

    @Builder.Default
    private Integer x_retention_user_ct = 0; //多日留存的数据

    @Builder.Default
    private Long active_user_ct = 0L; //活跃用户数

    @Builder.Default
    private Long recharge_user_ct = 0L; //充值用户数

    @Builder.Default
    private double recharge_amt = 0; //充值金额

    @Builder.Default
    private Long new_user_recharge_ct = 0L; //新增充值人数

    @Builder.Default
    private double new_user_recharge_amt = 0; //新增充值金额

    @Builder.Default
    private Long new_user_recharge_ct_dayretention = 0L; //新增充值人数次留

    @Builder.Default
    private Long new_user_recharge_cs_dayretention = 0L; //新增充值人数次留

    @Builder.Default
    private double new_user_recharge_amt_dayretention = 0; //新增充值金额次留

    @Builder.Default
    private Long old_user_recharge_ct = 0L; //老用户充值人数

    @Builder.Default
    private double old_user_recharge_amt = 0; //老用户充值金额

    @Builder.Default
    private Long withdrawal_user_ct = 0L; //提现用户数

    @Builder.Default
    private double withdrawal_amt = 0; //提现金额

    private Long dynamicDimension;//用于统计的 动态24小时维度

    //广告素材详情
    @Builder.Default
    private String source = "";

    @Builder.Default
    private String pkg = "";

    @Builder.Default
    private Long is_organic = 0L;

    @Builder.Default
    private String googleAdsCampaignType = ""; //GG1广告系列类型

    @Builder.Default
    private String googleAdsCampaignId = ""; //GG广告系列id

    @Builder.Default
    private String googleAdsCampaignName = ""; //GG广告系列名称

    @Builder.Default
    private String googleAdsAdgroupId = ""; // GG广告组id

    @Builder.Default
    private String googleAdsAdgroupName = ""; //GG广告组名称, always empty?

    @Builder.Default
    private String googleAdsCreativeId = ""; //GG广告素材id, always empty?

    @Builder.Default
    private String fbCampaignGroupName = ""; // FB广告系列名称

    @Builder.Default
    private String fbCampaignGroupId = ""; //FB广告系列id

    @Builder.Default
    private String fbCampaignName = ""; //FB广告组名称

    @Builder.Default
    private String fbCampaignId = ""; //FB广告组id

    @Builder.Default
    private String fbAdgroupName = ""; //FB广告素材名称

    @Builder.Default
    private String fbAdgroupId = ""; // FB广告素材id

    @Builder.Default
    private String pn = ""; //产品

    @Builder.Default
    private String uid = "";

    Long ts;
    private String hashTime;
}
