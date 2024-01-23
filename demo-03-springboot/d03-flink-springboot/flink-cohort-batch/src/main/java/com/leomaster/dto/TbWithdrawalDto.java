package com.leomaster.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户提现事件
 * 除了cds提供的业务数据，需要在map阶段获取到事件所属的推广素材等信息
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TbWithdrawalDto {

    private String correlationId;

    private String uid;

    private String gaid;

    private String channel;

    private long ctime; //提现订单创建时间

    private long mtime; //提现订单完成时间

    private double amount; //提现金额

    private String pn;

    //扩展的维度信息，用于后续的聚合计算
    private String adCampaignKey; //广告素材key
    private long withdrawalDate; //提现日期

    private long regDate; //用户真正的注册时间
    private long statRegDate; //统计用的用户注册时间，因为某些情况下，无法从user表中获取到用户的注册日期

    private Long dynamicDimension;//用于统计的 动态24小时维度

    private String hashTime;

    private long newRechargeDate;// 充值时间

    private String day;
}
