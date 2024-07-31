package com.yonyougov.yondif.job.dialect.mysql;

import com.alibaba.fastjson2.JSONObject;
import com.yonyougov.yondif.exception.YondifRuntimeException;
import com.yonyougov.yondif.job.dialect.IJdbcDialect;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.*;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MysqlJdbcDialect implements IJdbcDialect {
    private static final long serialVersionUID = -5335536588905606442L;

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public JdbcConnectionOptions getJdbcConnectionOptions(JSONObject finalDestSetting, String tableName, String databaseName) {
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(finalDestSetting.getString("driverclassname"))
                .withUrl("jdbc:mysql://" + finalDestSetting.getString("hostname") + ":" + finalDestSetting.getString("port") + "/" + databaseName)
                .withUsername(finalDestSetting.getString("username"))
                .withPassword(finalDestSetting.getString("password"))
                .build();
        return connectionOptions;
    }

    @Override
    public Optional<String> getUpsertStatement(String tableName, Set<String> fieldNames, String uniqueKeyFields) {
        String updateClause = fieldNames.stream()
                .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                .collect(Collectors.joining(", "));
        return Optional.of(
                getInsertIntoStatement(tableName, fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
                        + updateClause);
    }

    @Override
    public JdbcStatementBuilder<Message> createStatementBuilder(Map<String, String> tableFieldTypeMapping, String sql) {
        return (ps, message) -> {
            setParameterValue(ps, message, tableFieldTypeMapping);
        };
    }

    public String getInsertIntoStatement(String tableName, Set<String> fieldNames) {
        String columns = fieldNames.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = fieldNames.stream().map(f -> ":" + f).collect(Collectors.joining(", "));
        return "INSERT INTO "
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    private void setParameterValue(PreparedStatement ps, Message message, Map<String, String> tableFieldTypeMapping) {
        AtomicInteger i = new AtomicInteger(1);
        JSONObject body = message.getBody();
        tableFieldTypeMapping.keySet().forEach(key -> {
            String fieldType = tableFieldTypeMapping.get(key);
            try {
                if (body.get(key) == null) {
                    ps.setNull(i.getAndIncrement(), getJavaTypeForFieldType(fieldType));
                    return;
                }
                switch (fieldType.toLowerCase()) {
                    case "number":
                    case "numeric":
                    case "dec":
                    case "decimal":
                        ps.setBigDecimal(i.getAndIncrement(), body.getBigDecimal(key));
                        break;
                    case "char":
                    case "character":
                    case "varchar2":
                    case "rowid":
                    case "longvarchar":
                    case "varchar":
                        ps.setString(i.getAndIncrement(), body.getString(key));
                        break;
                    case "bit":
                        ps.setBoolean(i.getAndIncrement(), body.getBoolean(key));
                        break;
                    case "integer":
                    case "int":
                        ps.setInt(i.getAndIncrement(), body.getIntValue(key));
                        break;
                    case "bigint":
                        ps.setLong(i.getAndIncrement(), body.getLong(key));
                        break;
                    case "byte":
                    case "tinyint":
                        ps.setByte(i.getAndIncrement(), body.getByte(key));
                    case "smallint":
                        ps.setShort(i.getAndIncrement(), body.getShort(key));
                        break;
                    case "binary":
                    case "longvarbinary":
                    case "raw":
                    case "varbinary":
                        ps.setBytes(i.getAndIncrement(), body.getBytes(key));
                        break;
                    case "double precision":
                    case "float":
                    case "double":
                        ps.setDouble(i.getAndIncrement(), body.getDouble(key));
                        break;
                    case "real":
                        ps.setFloat(i.getAndIncrement(), body.getFloat(key));
                    case "date": // 转换为 java.sql.Date 类型，根据具体的日期格式进行解析
                        Date date = new Date(body.getDate(key).getTime());
                        ps.setDate(i.getAndIncrement(), date);
                        break;
                    case "datetime":
                    case "timestamp": // 转换为 java.sql.Timestamp 类型，根据具体的日期时间格式进行解析
                        Timestamp timestamp = new Timestamp(body.getDate(key).getTime());
                        ps.setTimestamp(i.getAndIncrement(), timestamp);
                        break;
                    case "time": // 转换为 java.sql.Time 类型，根据具体的时间格式进行解析
                        Time time = new Time(body.getDate(key).getTime());
                        ps.setTime(i.getAndIncrement(), time);
                        break;
                    case "long":
                    case "cblob":
                    case "text":
                        ps.setString(i.getAndIncrement(), body.getString(key));
                    default:
                        throw new UnsupportedOperationException("Unsupported type:" + fieldType);
                }
            } catch (Exception e) {
                throw new YondifRuntimeException(String.format("convert data type exception,key:{%s},fieldType:{%s}", key, fieldType), e);
            }
        });
    }


    private int getJavaTypeForFieldType(String fieldType) {
        switch (fieldType.toLowerCase()) {
            case "number":
                return Types.NUMERIC;
            case "numeric":
                return Types.NUMERIC;
            case "dec":
                return Types.DECIMAL;
            case "decimal":
                return Types.DECIMAL;
            case "char":
                return Types.CHAR;
            case "character":
                return Types.CHAR;
            case "varchar2":
                return Types.VARCHAR;
            case "rowid":
                return Types.ROWID;
            case "longvarchar":
                return Types.LONGVARCHAR;
            case "varchar":
                return Types.VARCHAR;
            case "bit":
                return Types.BIT;
            case "integer":
                return Types.INTEGER;
            case "int":
                return Types.INTEGER;
            case "bigint":
                return Types.BIGINT;
            case "byte":
                return Types.TINYINT;
            case "tinyint":
                return Types.TINYINT;
            case "smallint":
                return Types.SMALLINT;
            case "binary":
                return Types.BINARY;
            case "raw":
                return Types.BINARY;
            case "varbinary":
                return Types.VARBINARY;
            case "float":
                return Types.DOUBLE;
            case "double":
                return Types.DOUBLE;
            case "double precision":
                return Types.DOUBLE;
            case "real":
                return Types.FLOAT;
            case "date":
                return Types.DATE;
            case "time":
                return Types.TIME;
            case "datetime":
                return Types.TIMESTAMP;
            case "timestamp":
                return Types.TIMESTAMP;
            case "long":
                return Types.LONGVARCHAR;
            case "clob":
                return Types.CLOB;
            case "text":
                return Types.LONGVARCHAR;
            default:
                return Types.NULL;
        }
    }
}
