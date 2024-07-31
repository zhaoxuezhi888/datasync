package com.yonyougov.yondif.job.flatmap;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import com.yonyougov.yondif.config.dboperator.AsyncSQLWriter;
import com.yonyougov.yondif.job.flatmap.entity.AmsDatasyncExceptionMessage;
import com.yonyougov.yondif.job.flatmap.entity.DmlType;
import com.yonyougov.yondif.log.ExceptionMessageCreator;
import com.yonyougov.yondif.log.LogErrorCode;
import com.yonyougov.yondif.log.LogField;
import com.yonyougov.yondif.log.LogMarker;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author zxz
 * @description 对接收的kafka的数据进行必须的过滤和验证
 * @return
 * @date 2022年07月23日 13:28
 */
public class RowFlatMap extends BaseFlatMap<Row> {
    private static final Logger logger = LogManager.getLogger(RowFlatMap.class);
    private static final long serialVersionUID = 1L;
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

    /*
     * 返回内容为
     * */
    @Override
    public void flatMap(ConsumerRecord<String, String> input, Collector<Row> out) {
        try {
            //入口添加跟踪日志
            if (parameterTool.getInt("trace_log", 0) == 1) {
                logger.info(LogMarker.trace_marker, input.toString());
            }
            JSONObject value = convertToJson(input);
            if (value == null) {
                return;
            }
            DmlType dmlType = getDmlType(input, value);
            if (dmlType == null) {
                return;
            }
            String tableName = getTableName(input, value);
            if (tableName == null) {
                return;
            }
            String databaseName = getDatabaseName(input, value);
            String indexName = getOriIndexName(input, tableName);

            JSONObject body = getMessageBody(input, value, dmlType);
            if (body == null) {
                AmsDatasyncExceptionMessage amsDatasyncExceptionMessage = ExceptionMessageCreator.create(LogErrorCode.BODY_IS_NULL, value.toJSONString(), "body不能为空");
                AsyncSQLWriter.writeDataAsync(amsDatasyncExceptionMessage);
                return;
            }


            String primaryKey = getPrimaryKey(input, tableName);
            if (primaryKey == null) {
                AmsDatasyncExceptionMessage amsDatasyncExceptionMessage = ExceptionMessageCreator.create(LogErrorCode.PK_IS_NULL, value.toJSONString(), "主键不能为空");
                AsyncSQLWriter.writeDataAsync(amsDatasyncExceptionMessage);
                return;
            }
            String primaryValue = getPrimaryValue(input, body, primaryKey);
            if (primaryValue == null) {
                AmsDatasyncExceptionMessage amsDatasyncExceptionMessage = ExceptionMessageCreator.create(LogErrorCode.PK_VALUE_IS_NULL, value.toJSONString(), "主键值不能为空");
                AsyncSQLWriter.writeDataAsync(amsDatasyncExceptionMessage);
                return;
            }
            String destDatabase = parameterTool.get(tableName + "_dest_database_field");
            //获取字段映射表
            Map<String, Object> fieldMapping = tableFieldMappings.get(tableName);
            //转换key和value值
            Row row = null;

            if (org.apache.commons.collections.MapUtils.isNotEmpty(fieldMapping)) {
                JSONObject newBody = ConvertUtil.toMappingField(body, fieldMapping);
                row = ConvertUtil.maptoRowConvert(newBody);
                switch (dmlType) {
                    case INSERT:
                        row.setKind(RowKind.INSERT);
                    case UPDATE:
                        row.setKind(RowKind.UPDATE_AFTER);
                        break;
                    case DELETE:
                        row.setKind(RowKind.DELETE);
                        break;
                    default:
                        break;
                }
            }
            out.collect(row);
        } catch (Exception e) {
            logger.error(LogMarker.error_marker, input.value(), e);
            return;
        }
    }

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
        String primaryKey = parameterTool.get(tableName + "_es_primary_field");
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

    private String getOpTime(ConsumerRecord<String, String> input, JSONObject value) {
        String opTime = value.getString("OP_TIME");
        return opTime;
    }

    private String getLoaderTime(ConsumerRecord<String, String> input, JSONObject value) {
        String loaderTime = value.getString("LOADERTIME");
        return loaderTime;
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
