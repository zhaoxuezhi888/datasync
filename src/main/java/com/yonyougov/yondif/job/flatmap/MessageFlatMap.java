package com.yonyougov.yondif.job.flatmap;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import com.yonyougov.yondif.job.flatmap.entity.DmlType;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import com.yonyougov.yondif.log.LogErrorCode;
import com.yonyougov.yondif.log.LogField;
import com.yonyougov.yondif.log.LogMarker;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Author zxz
 * @description 对接收的kafka的数据进行必须的过滤和验证
 * @return
 * @date 2022年07月23日 13:28
 */
public abstract class MessageFlatMap extends BaseFlatMap {
    private static final Logger logger = LogManager.getLogger(MessageFlatMap.class);
    private static final long serialVersionUID = 1L;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    /*
     * 返回内容为
     * */
    @Override
    public void flatMap(ConsumerRecord<String, String> input, Collector<Message> out) {
        try {
            //入口添加跟踪日志
            if (parameterTool.getInt("trace_log", 0) == 1) {
                logger.info(LogMarker.trace_marker, input.toString());
            }
            Message message = new Message();
            message.setTarceId(input.key());
            message.setInFlinkTime(getinFlinkTime(input));
            message.setError(false);
            JSONObject value = convertToJson(input);
            if (value == null) {
                return;
            }
            message.setOpTime(getOpTime(value));
            message.setInKafkaTime(getinKafkaTime(value));
            DmlType dmlType = getDmlType(input, value);
            if (dmlType == null) {
                return;
            }
            message.setDmlType(dmlType);
            JSONObject body = getMessageBody(input, value, dmlType);
            if (body == null) {
                message.setError(true);
                message.setErrorCode(LogErrorCode.BODY_IS_NULL);
                message.setMessage("body内容为空");
                out.collect(message);
                return;
            }
            message.setBody(body);
            String tableName = getTableName(input, value);
            if (tableName == null) {
                message.setError(true);
                message.setErrorCode(LogErrorCode.TABLE_IS_NULL);
                message.setMessage("table内容为空");
                out.collect(message);
                return;
            }
            message.setTableName(tableName);
            String databaseName = getDatabaseName(input, value);
            message.setDatabaseName(databaseName);
            String indexName = getOriIndexName(input, tableName);
            message.setIndexName(indexName);
            String primaryKey = getPrimaryKey(input, tableName);
            if (primaryKey == null) {
                message.setError(true);
                message.setErrorCode(LogErrorCode.PK_IS_NULL);
                message.setMessage("pk为空");
                out.collect(message);
                return;
            }
            message.setPrimaryKey(primaryKey);
            String primaryValue = getPrimaryValue(input, body, primaryKey);
            if (primaryValue == null) {
                message.setError(true);
                message.setErrorCode(LogErrorCode.PK_VALUE_IS_NULL);
                message.setMessage("pk值为空");
                out.collect(message);
                return;
            }
            message.setPrimaryValue(primaryValue);
            String destDatabase = parameterTool.get(tableName + "_dest_database_field");
            message.setDestDatabase(destDatabase);
            flatMapAfter(message);
            out.collect(message);
        } catch (Exception e) {
            logger.error(LogMarker.error_marker, input.value(), e);
            return;
        }
    }

    /**
     * 消息进一步处理
     */
    public abstract Message flatMapAfter(Message message);

    private String getPrimaryValue(ConsumerRecord<String, String> input, JSONObject body, String primaryKey) {
        String primaryValue = body.getString(primaryKey);
        if (StringUtils.isBlank(primaryValue)) {
            logger.error(LogMarker.error_marker, input.value(), new RuntimeException("primaryValue不能为空"));
            return null;
        }
        ThreadContext.put(LogField.PK_ID, primaryValue);
        return primaryValue;
    }

    private String getPrimaryKey(ConsumerRecord<String, String> input, String tableName) {
        String primaryKey = parameterTool.get(tableName + "_primary_field");
        if (StringUtils.isBlank(primaryKey)) {
            logger.error(LogMarker.error_marker, input.value(), new RuntimeException("primaryKey不能为空"));
            return null;
        }
        return primaryKey;
    }

    private JSONObject getMessageBody(ConsumerRecord<String, String> input, JSONObject value, DmlType dmlType) {
        JSONObject body = null;
        switch (dmlType) {
            case INSERT:
                body = value.getJSONObject("columnInfo");
                break;
            case UPDATE:
                body = value.getJSONObject("columnsAfter");
                break;
            case DELETE:
                body = value.getJSONObject("columnInfo");
                break;
            default:
                break;
        }
        if (MapUtils.isEmpty(body)) {
            return null;
        }
        return body;
    }

    private String getOriIndexName(ConsumerRecord<String, String> input, String tableName) {
        String indexName = parameterTool.get(tableName + "_index");
        if (StringUtils.isBlank(indexName)) {
            logger.error(LogMarker.error_marker, input.value(), new RuntimeException("indexName不能为空"));
            return null;
        }
        ThreadContext.put(LogField.INDEX_NAME, indexName);
        return indexName;
    }

    private String getDatabaseName(ConsumerRecord<String, String> input, JSONObject value) {
        String databaseName = value.getString("OWNER");
        if (StringUtils.isBlank(databaseName)) {
            logger.error(LogMarker.error_marker, input.value(), new FlinkRuntimeException(String.format("库名%s为空，请排查数据来源", databaseName)));
            return null;
        }
        ThreadContext.put(LogField.DATABASE_NAME, databaseName);
        return databaseName;
    }

    private String getTableName(ConsumerRecord<String, String> input, JSONObject value) {
        String tableName = value.getString("TABLE");
        if (StringUtils.isBlank(tableName) || !tableList.containsKey(tableName)) {
            logger.error(LogMarker.error_marker, input.value(), new FlinkRuntimeException(String.format("%s表不在此job范围，请排查数据来源", tableName)));
            return null;
        }
        ThreadContext.put(LogField.TABLE_NAME, tableName);
        return tableName;
    }

    private String getOpTime( JSONObject value) {
        return value.getString("OP_TIME");
    }

    private String getinKafkaTime(JSONObject value) {
        return value.getString("LOADERTIME");
    }

    private String getinFlinkTime(ConsumerRecord<String, String> input) {
        Instant instant = Instant.ofEpochMilli(input.timestamp());
        ZoneId zone = ZoneId.systemDefault();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
        return localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    private DmlType getDmlType(ConsumerRecord<String, String> input, JSONObject value) {
        DmlType dmlType = DmlType.fromString(value.getString("operType"));
        if (dmlType == null) {
            logger.error(LogMarker.error_marker, value);
            return null;
        }
        return dmlType;
    }

    private JSONObject convertToJson(ConsumerRecord<String, String> input) {
        JSONObject value = null;
        try {
            value = JSONObject.parseObject(input.value(), JSONObject.class, JSONReader.Feature.FieldBased, JSONReader.Feature.UseBigDecimalForFloats, JSONReader.Feature.UseBigDecimalForFloats);
        } catch (Exception e) {
            logger.error(LogMarker.error_marker, input.value(), e);
        }
        return value;
    }

    /*
        算子关闭时，清理日志的threadcontext
    */
    @Override
    public void close() {
        super.close();
    }
}
