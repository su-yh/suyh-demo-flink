package com.leomaster.utils;


import com.leomaster.core.constants.PN;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Author: Felix
 * Date: 2021/2/20
 * Desc:  日期转换的工具类
 * SimpleDateFormat存在线程安全问题,底层调用 calendar.setTime(date);
 * 解决：在JDK8，提供了DateTimeFormatter替代SimpleDateFormat
 * TODO 程序运行的环境默认时区为UTC+0
 */
public class DateTimeUtil {

    public static Long toTs(String dateStr, String format) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        try {
            Date date = simpleDateFormat.parse(dateStr);
            return date.getTime();
        } catch (ParseException e) {
        }
        return -1L;
    }

    /**
     * 日期格式化 日期格式为：yyyy-MM-dd
     *
     * @param date    日期
     * @param pattern 格式，如：DateUtils.DATE_TIME_PATTERN
     * @return 返回yyyy-MM-dd格式日期
     */
    public static String format(Date date, String pattern) {
        if (date != null) {
            SimpleDateFormat df = new SimpleDateFormat(pattern);
            df.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
            return df.format(date);
        }
        return null;
    }

    public static Integer toIntDate(long timeStamp) {
        try {
            return Integer.parseInt(format(new Date(timeStamp), "yyyyMMdd"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 转换事件的时间戳，由于aw的数据结算需要按照印度新德里时区(+5.5)进行，所以需要根据不同的pn进行转换
     * FIXME 这里直接偏移时间戳的方式来处理时区问题并不合理，后面有时间再改
     *
     * @param eventTimestamp
     * @param pn
     * @return
     */
    public static long transEventTimestamp(long eventTimestamp, String pn) {
        if (PN.HY.Name.equals(pn)) {
            return (eventTimestamp + 28800) * 1000; //8 * 3600 = 28800;
        }
        return (eventTimestamp + 19800) * 1000; //5.5 * 3600;  or 9000
    }

    public static Integer deltaDays(long date1, long date2) {

        if(0 ==date1 || 0==date2){
            return -1;
        }

        if(-1L ==date1 || -1L==date2){
            return -1;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        try {
            Date d1 = sdf.parse(date1 + "");
            Date d2 = sdf.parse(date2 + "");

            int days = (int) ((d1.getTime() - d2.getTime()) / (1000 * 3600 * 24));
            return days;
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return Integer.MAX_VALUE;
    }

    private static Integer num = 24 * 3600*1000;

    public static Long queryTimeDay(long regDateTime, long rechargeCtime) {

        long difference = rechargeCtime - regDateTime;

        long quotient = (difference) / (num);
        //long remainder = (difference) % (num);

        return quotient;
        //return Long.valueOf(toIntDate((regDateTime + num *l)*1000));
    }

    public static Long toMilliTs(String dateStr) {

        if(StringUtils.isEmpty(dateStr)){
            return null;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        try {
            Date date = simpleDateFormat.parse(dateStr);
            return date.getTime();
        } catch (ParseException e) {
        }
        return null;
    }

    private static long minute = 10*60*1000;
    public static String toHashTime(long timeStamp) {
        try {
            long l = (timeStamp / minute) * minute;
            return format(new Date(l), "yyyyMMddHHmm");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
