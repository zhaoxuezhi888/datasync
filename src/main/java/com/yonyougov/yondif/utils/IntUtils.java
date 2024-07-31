package com.yonyougov.yondif.utils;

/**
 * @Author zxz
 * @description
 * @date 2022年11月02日 10:47
 */
public class IntUtils {
    public static int getIntValue(int value, int defaultValue) {
        if (value == 0) {
            return defaultValue;
        }
        return value;
    }

    public static Long getLongValue(Long value, Long defaultValue) {
        if (value == 0L) {
            return defaultValue;
        }
        return value;
    }
}
