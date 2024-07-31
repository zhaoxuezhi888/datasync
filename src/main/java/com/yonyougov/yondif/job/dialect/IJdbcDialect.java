package com.yonyougov.yondif.job.dialect;

import com.alibaba.fastjson2.JSONObject;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author zxz
 */
public interface IJdbcDialect extends Serializable {
    /**
     * 如果列名是保留关键字或包含需要加引号的字符(例如空格)，则在标识符周围加引号
     */
    String quoteIdentifier(String identifier);

    /**
     * 获取连接字符串
     */
    JdbcConnectionOptions getJdbcConnectionOptions(JSONObject destSetting, String tableName, String databaseName);

    /**
     * 获取upsert SQL
     */
    Optional<String> getUpsertStatement(String tableName, Set<String> fieldNames, String uniqueKeyFields);

    /**
     * 创建parperstatement
     */
    JdbcStatementBuilder<Message> createStatementBuilder(Map<String, String> tableFieldTypeMapping, String sql);

    /**
     *  读取相关的元数据
     */
//    JdbcStatementBuilder<Message> createStatementBuilder(JSONObject tableFieldTypeMapping);
}
