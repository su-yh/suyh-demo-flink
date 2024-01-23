package com.leomaster.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户登录事件
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TbUserLoginDto {

    private String correlationId;

    private String uid;

    private String gaid;

    private String channel;

    private long ctime;

    private String pn;

    private String adCampaignKey;

    private long loginDate;

    private long regDate; //用户真正的注册时间

    private String userRegChannel; //用户的注册渠道

    private long statRegDate; //统计用的用户注册时间，因为某些情况下，无法从user表中获取到用户的注册日期

    private String day;

    private String hashTime;

    private long newRechargeDate;// 充值时间

    private Long dynamicDimension;//用于统计的 动态24小时维度
}
