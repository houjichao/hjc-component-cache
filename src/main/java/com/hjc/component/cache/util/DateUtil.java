package com.hjc.component.cache.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 时间工具类
 * 
 * @author hjc
 * 
 */
public class DateUtil {
    public final static String YYYYMMDD         = "yyyyMMdd";
    public final static String YYYYMMDDHH24MISS = "yyyyMMddHHmmss";
    public final static String HH24MISS         = "HHmmss";
    
    /**
     * 获取当前时间
     * 
     * @return 返回时间格式为YYYYMMDDHH24MISS的字符串
     */
    public static String getCurDate() {
        return date2Str(new Date(), YYYYMMDDHH24MISS);
    }
    
    /**
     * 获取当前时间
     * 
     * @param dateFormat
     *            时间格式
     * @return 返回指定时间格式的字符串
     */
    public static String getCurDate(String dateFormat) {
        return date2Str(new Date(), dateFormat);
    }
    
    /**
     * 将时间格式为YYYYMMDDHH24MISS的字符串转化为Date
     * 
     * @param dateStr
     *            时间格式为YYYYMMDDHH24MISS的字符串
     * @return Date
     */
    public static Date str2Date(String dateStr) {
        return str2Date(dateStr, YYYYMMDDHH24MISS);
    }
    
    /**
     * 时间串转化为Date
     * 
     * @param dateStr
     *            dateFormat时间格式的字符串
     * @param dateFormat
     *            时间格式
     * @return Date
     */
    public static Date str2Date(String dateStr, String dateFormat) {
        if (StringHelper.isEmpty(dateStr)) {
            return null;
        }
        
        SimpleDateFormat df = new SimpleDateFormat(dateFormat);
        try {
            return df.parse(dateStr);
        } catch (Exception ex) {
            return null;
        }
    }
    
    /**
     * Date转化为YYYYMMDDHH24MISS格式的字符串
     * 
     * @param date
     *            Date
     * @return YYYYMMDDHH24MISS格式的字符串
     */
    public static String date2Str(Date date) {
        return date2Str(date, YYYYMMDDHH24MISS);
    }
    
    /**
     * Date转化为dateFormat时间格式的字符串
     * 
     * @param date
     *            Date
     * @param dateFormat
     *            时间格式
     * @return dateFormat时间格式的字符串
     */
    public static String date2Str(Date date, String dateFormat) {
        if (date == null) {
            return null;
        }
        SimpleDateFormat df = new SimpleDateFormat(dateFormat);
        return df.format(date);
    }
}
