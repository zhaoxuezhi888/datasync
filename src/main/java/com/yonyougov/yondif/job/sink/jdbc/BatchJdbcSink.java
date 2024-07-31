package com.yonyougov.yondif.job.sink.jdbc;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.yonyougov.yondif.exception.YondifRuntimeException;
import com.yonyougov.yondif.job.dialect.IJdbcDialect;
import com.yonyougov.yondif.job.dialect.dm.datastream.DmJdbcDialect;
import com.yonyougov.yondif.job.dialect.mysql.MysqlJdbcDialect;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import com.yonyougov.yondif.job.sink.SinkFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class BatchJdbcSink extends SinkFactory<Map<String, SinkFunction<Message>>, String> {
    private static final Logger logger = LogManager.getLogger(BatchJdbcSink.class);
    private String jdbcType;

    public BatchJdbcSink(StreamExecutionEnvironment env, ParameterTool parameterTool, String jdbcType) {
        super(env, parameterTool);
        this.jdbcType = jdbcType;
    }

    @Override
    public Map<String, SinkFunction<Message>> createSink(String... args) throws RuntimeException {
        JSONObject destSetting;
        try {
            JSONObject destSource = JSON.parseObject(parameterTool.get("source_" + parameterTool.getLong("dest_id")));
            destSetting = JSON.parseObject(destSource.getString("source_setting"));
        } catch (Exception e) {
            throw new YondifRuntimeException("Sink目标地址获取失败");
        }
        if (args == null && args.length == 0) {
            throw new YondifRuntimeException("tablename不能为空");
        }
        Map<String, SinkFunction<Message>> jdbcSinks = new HashMap<>();
        JSONObject finalDestSetting = destSetting;
        IJdbcDialect jdbcDialect = getJdbcDialect();

        String[] argArr = args[0].split(",");
        List<String> argList = Arrays.asList(argArr);
        argList.forEach(tableName -> {
            //拼接主键
            String primaryKey = parameterTool.get(tableName + "_primary_field");
            String databaseName = parameterTool.get(tableName + "_database_name");
            Map<String, String> tableFieldMapping = JSON.parseObject(parameterTool.get(tableName + "_field"), Map.class);
            //从元数据中获得的字段类型
            Map<String, String> tableFieldTypeMapping = JSON.parseObject(parameterTool.get(tableName + "_field_type")).toJavaObject(Map.class);
            //将元数据中不包括的字段去过滤掉
            Map<String, String> filteredMap = new HashMap<>();
            tableFieldMapping.keySet().forEach(key -> {
                String destField = tableFieldMapping.get(key);
                filteredMap.put(destField, tableFieldTypeMapping.get(key));
            });
            JdbcConnectionOptions connectionOptions = jdbcDialect.getJdbcConnectionOptions(finalDestSetting, tableName, databaseName);
            Optional<String> mergeSql = jdbcDialect.getUpsertStatement(tableName, filteredMap.keySet(), primaryKey);
            JdbcStatementBuilder<Message> statementBuilder = jdbcDialect.createStatementBuilder(filteredMap, mergeSql.get());

            SinkFunction<Message> jdbcSink = JdbcSink.sink(
                    mergeSql.get(),
                    statementBuilder,
                    JdbcExecutionOptions.builder()
                            .withBatchSize(3)
                            .withBatchIntervalMs(5000)
                            .withMaxRetries(1)
                            .build(),
                    connectionOptions);
            jdbcSinks.put(tableName, jdbcSink);
        });
        return jdbcSinks;
    }

    private IJdbcDialect getJdbcDialect() {
        IJdbcDialect jdbcDialect;
        switch (jdbcType) {
            case "dm":
                jdbcDialect = new DmJdbcDialect();
                break;
            default:
                jdbcDialect = new MysqlJdbcDialect();
                break;
        }
        return jdbcDialect;
    }
}
