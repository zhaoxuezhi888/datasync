package com.yonyougov.yondif.job.flatmap;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @Author zxz
 * @description 工具类
 * @date 2022年11月03日 14:55
 */
public class ConvertUtil {
    private static final Logger logger = LogManager.getLogger(ConvertUtil.class);

    /**
     * 把map的key转换成mapping中的值
     */
    public static JSONObject toMappingField(JSONObject map, Map<String, Object> fieldMapping) {
        JSONObject reMap = new JSONObject();
        Iterator<Map.Entry<String, Object>> item = map.entrySet().iterator();
        while (item.hasNext()) {
            Map.Entry<String, Object> entry = item.next();
            //重命名字段
            String key = entry.getKey();
            Object value = entry.getValue();
            if (fieldMapping.containsKey(key)) {
                String mappingKey = String.valueOf(fieldMapping.get(key));
                //特殊字符过滤
                if (isAbsoluteBlank(String.valueOf(value))) {
                    value = null;
                }
                if (value instanceof Boolean) {
                    if (value != null) {
                        if ((Boolean) value) {
                            value = 1;
                        } else {
                            value = 0;
                        }
                    }
                }
                //转换带T的日期格式
                if (value instanceof Timestamp) {
                    if (value != null) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        value = sdf.format(value);
                    }
                }
                if (value instanceof LocalDateTime) {
                    if (value != null) {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        value = ((LocalDateTime) value).format(formatter);
                    }
                }
                reMap.put(mappingKey, value);
            }
        }
        return reMap;
    }

    public static boolean isAbsoluteBlank(String str) {
        if (isEmpty(str)) {
            return false;
        }
        int strLen = str.length();
        char c;
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(c = str.charAt(i)) && c != '\u0000') {
                return false;
            }
        }
        return true;
    }

    public static Row maptoRowConvert(Map<String, Object> map) {
        Row row = new Row(map.size());
        int index = 0;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().equals("id") || entry.getKey().equals("age")) {
                int id = Integer.parseInt(entry.getValue().toString());
                row.setField(index++, id);
            } else if (entry.getKey().equals("name")) {
                row.setField(index++, entry.getValue().toString());
            } else {
                row.setField(index++, entry.getValue().toString());
            }
        }
        return row;
    }
}
