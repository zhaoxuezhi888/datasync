package com.yonyougov.yondif.job.flatmap;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import com.yonyougov.yondif.log.LogErrorCode;
import com.yonyougov.yondif.log.LogMarker;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class Es7FlatMap extends MessageFlatMap {
    private static final Logger logger = LogManager.getLogger(Es7FlatMap.class);
    private Map<String, Map<String, Object>> tableFieldMappings = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JSONObject tableList = JSON.parseObject(parameterTool.get("table_list"));
        for (String key : tableList.keySet()) {
            JSONObject fieldMapping = JSON.parseObject(parameterTool.get(key + "_field"));
            tableFieldMappings.put(key, fieldMapping);
        }
    }

    /**
     * 将消息key转为驼峰，并过滤空值
     */
    @Override
    public Message flatMapAfter(Message message) {
        Map<String, Object> fieldMapping = tableFieldMappings.get(message.getTableName());
        JSONObject body = message.getBody();
        //字段映射转换,过滤特殊字符
        if (MapUtils.isNotEmpty(fieldMapping)) {
            JSONObject newBody = null;
            try {
                newBody = ConvertUtil.toMappingField(body, fieldMapping);
                message.setBody(newBody);
            } catch (Exception e) {
                logger.error(LogMarker.error_marker, body, e);
                message.setError(true);
                message.setErrorCode(LogErrorCode.CONVERT_MESSAGE_EXCEPTION);
                message.setMessage("消息转换异常");
                message.setException(e.getMessage());
            }
        }
        return message;
    }
}
