package com.suyh.springboot.common.constants;

public class ConstantUtils {

    static final String sourceQueueUserLoginTable = "live_tb_user_login";

    static final String washSinkQueueUserLoginTable = "wash_tb_user_login";


    public final static int DIM_TIMEOUT_SECONDS = 60000; //先设置大一点，避免跨网环境下的任务超时触发失败

    public final static int DIM_CAPACITY = 100;


    public final static int WATERMARKS_OFFSET_HOUR = 0;

    public final static int WATERMARKS_OFFSET_HOURUTC = 0;

    public final static int STAT_WINDOW_TRIGGER_SECS = 60;

    public final static int STAT_WINDOW_TRIGGER_SECS_TIME = 10;

    public final static Long CHECKPOINT_INTERVAL = 2 * 60 * 1000L; // 3 MIN;

//    public final static String POLY_TB_USER_LOGIN = "poly_tb_user_login";
    public final static String POLY_TB_USER_LOGIN = "poly_tb_user_login_pre";

//    public final static String POLY_TB_USER = "poly_tb_user";
    public final static String POLY_TB_USER = "poly_tb_user_pre";

//    public final static String POLY_TB_RECHARGE = "poly_tb_recharge";
    public final static String POLY_TB_RECHARGE = "poly_tb_recharge_pre";

//    public final static String POLY_TB_WITHDRAWAL = "poly_tb_withdrawal";
    public final static String POLY_TB_WITHDRAWAL = "poly_tb_withdrawal_pre";

    public final static String TMP = "/opt/trend_oper/tmp/";

}
