package com.yonyougov.yondif.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Author zxz
 * @description
 * @date 2022年07月12日 20:27
 */
public class DateTimeUtils {
    private static final String DATE_TIME_STR = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_MillTIME_STR = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final String DATE_NanoTIME_STR = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final String DATE_STR = "yyyy-MM-dd";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_TIME_STR);
    private static final DateTimeFormatter DATE_MillTIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_MillTIME_STR);
    private static final DateTimeFormatter DATE_NanoTIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_NanoTIME_STR);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_STR);

    /**
     * 字符串转时间戳到毫秒级
     *
     * @param time 时间串
     * @return long 毫秒级时间戳
     */
    public static Long toLongDateStr(String time) {
        if (time == null || time.length() == 0) {
            return 0L;
        }
        if (time.length() == DATE_TIME_STR.length()) {
            LocalDateTime dateTime = stringToDateTime(time);
            return dateTimeToEpochMilli(dateTime);
        }
        if (time.length() == DATE_MillTIME_STR.length()) {
            LocalDateTime dateTime = stringToMillDateTime(time);
            return dateTimeToEpochMilli(dateTime);
        }
        if (time.length() == DATE_NanoTIME_STR.length()) {
            LocalDateTime dateTime = stringToNanoDateTime(time);
            return dateTimeToEpochMilli(dateTime);
        }
        LocalDate date = stringToDate(time);
        return dateToEpochMilli(date);
    }

    /**
     * 获取到毫秒级时间戳
     *
     * @param localDate 具体时间
     * @return long 毫秒级时间戳
     */
    public static long dateToEpochMilli(LocalDate localDate) {
        return localDate.atStartOfDay(ZoneOffset.ofHours(8)).toInstant().toEpochMilli();
    }

    /**
     * 获取到毫秒级时间戳
     *
     * @param localDateTime 具体时间
     * @return long 毫秒级时间戳
     */
    public static long dateTimeToEpochMilli(LocalDateTime localDateTime) {
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }


    /**
     * 时间字符串 转LocalDateTime
     *
     * @param localDateTime 时间字符串
     * @return LocalDateTime
     */
    public static LocalDateTime stringToDateTime(String localDateTime) {
        return LocalDateTime.parse(localDateTime, DATE_TIME_FORMATTER);
    }

    /**
     * 时间字符串 转LocalDateTime
     *
     * @param localDateTime 时间字符串
     * @return LocalDateTime
     */
    public static LocalDateTime stringToMillDateTime(String localDateTime) {
        return LocalDateTime.parse(localDateTime, DATE_MillTIME_FORMATTER);
    }

    /**
     * 时间字符串 转LocalDateTime
     *
     * @param localDateTime 时间字符串
     * @return LocalDateTime
     */
    public static LocalDateTime stringToNanoDateTime(String localDateTime) {
        return LocalDateTime.parse(localDateTime, DATE_NanoTIME_FORMATTER);
    }

    /**
     * 时间字符串 转LocalDateTime
     *
     * @param localDateTime 时间字符串
     * @return LocalDateTime
     */
    public static LocalDate stringToDate(String localDateTime) {
        return LocalDate.parse(localDateTime, DATE_FORMATTER);
    }


    /**
     * 转换带T的日期的格式
     *
     * @param timet 时间串
     */
    public static String formatIncludeTTime(String timet) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            SimpleDateFormat sdf2 = new SimpleDateFormat(DATE_TIME_STR);
            Date date = sdf1.parse(timet);
            return sdf2.format(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return timet;
    }

    /**
     * 判断是否是时间格式
     *
     * @param dateString 时间串
     * @return boolean 是否是时间格式
     */
    public static boolean isValidDateStr(String dateString) {
        SimpleDateFormat df = new SimpleDateFormat(DATE_STR);
        try {
            df.parse(dateString);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * 字符串时间戳转日期格式
     *
     * @param timestamp 字符串时间戳转
     * @return String 日期格式
     */
    public static String formatTimeStamp(String timestamp) {
        // Instant.ofEpochMilli 转 毫秒，  Instant.ofEpochSecond 转 秒
        LocalDateTime localDateTime = Instant.ofEpochMilli(Long.parseLong(timestamp)).atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        return localDateTime.format(DATE_TIME_FORMATTER);
    }


    /**
     * 判断字符串内容是否为时间串
     *
     * @param timestamp 字符串时间戳转
     * @return boolean 是否为日期格式
     */
    public static boolean isValidTimeStamp(String timestamp) {
        SimpleDateFormat df = new SimpleDateFormat(DATE_TIME_STR);
        try {
            df.format(new Date(Long.parseLong(String.valueOf(timestamp))));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
