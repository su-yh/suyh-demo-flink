package com.suyhd13.vo;

import lombok.Data;

@Data
public class TbUserDto {

    private String correlationId;

    private String uid;

    private String gaid;

    private String channel;

    private Long ctime;

    private String pn;

    private String adCampaignKey;

    private Long regDate;

    private Long statRegDate;

    private String hashTime;

    private Long day;

    private Long dynamicDimension;
}
