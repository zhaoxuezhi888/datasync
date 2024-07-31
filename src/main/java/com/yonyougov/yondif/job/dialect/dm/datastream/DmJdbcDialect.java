package com.yonyougov.yondif.job.dialect.dm.datastream;

import com.alibaba.fastjson2.JSONObject;
import com.yonyougov.yondif.exception.YondifRuntimeException;
import com.yonyougov.yondif.job.dialect.IJdbcDialect;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author zxz
 */
public class DmJdbcDialect implements IJdbcDialect {
    private static final long serialVersionUID = 5117309029290410061L;
    private static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public JdbcConnectionOptions getJdbcConnectionOptions(JSONObject finalDestSetting, String tableName, String databaseName) {
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(finalDestSetting.getString("driverclassname"))
                .withUrl("jdbc:dm://" + finalDestSetting.getString("hostname") + ":" + finalDestSetting.getString("port") + "/" + databaseName+"?logLevel=info&logDir=D:\\data\\dmlog")
                .withUsername(finalDestSetting.getString("username"))
                .withPassword(finalDestSetting.getString("password"))
                .build();
        return connectionOptions;
    }

    @Override
    public Optional<String> getUpsertStatement(String tableName, Set<String> fieldNames, String uniqueKeyFields) {
        String sourceFields =
                fieldNames.stream()
                        .map(f -> "? AS " + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String onClause = "t." + quoteIdentifier(uniqueKeyFields) + "=s." + quoteIdentifier(uniqueKeyFields);

        String updateClause =
                fieldNames.stream()
                        .filter(f -> !uniqueKeyFields.equalsIgnoreCase(f))
                        .map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String insertFields =
                fieldNames.stream()
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String valuesClause =
                fieldNames.stream()
                        .map(f -> "s." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        String mergeQuery =
                " MERGE INTO "
                        + quoteIdentifier(tableName)
                        + " t "
                        + " USING (SELECT "
                        + sourceFields
                        + " FROM DUAL) s "
                        + " ON ("
                        + onClause
                        + ") "
                        + " WHEN MATCHED THEN UPDATE SET "
                        + updateClause
                        + " WHEN NOT MATCHED THEN INSERT ("
                        + insertFields
                        + ")"
                        + " VALUES ("
                        + valuesClause
                        + ")";

        return Optional.of(mergeQuery);
    }

    @Override
    public JdbcStatementBuilder<Message> createStatementBuilder(Map<String, String> tableFieldTypeMapping, String sql) {
        return (ps, message) -> {
            ps.clearParameters();
            setParameterValue(ps, message, tableFieldTypeMapping);
        };
    }

    private void setParameterValue(PreparedStatement ps, Message message, Map<String, String> tableFieldTypeMapping) {
        AtomicInteger i = new AtomicInteger(1);
        JSONObject body = message.getBody();
        tableFieldTypeMapping.keySet().forEach(key -> {
            String fieldType = tableFieldTypeMapping.get(key);

            try {
                if (!body.containsKey(key) || body.get(key) == null) {
                    ps.setNull(i.getAndIncrement(), Types.VARCHAR);
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
                        break;
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
                        break;
                    case "date":
                        ps.setDate(i.getAndIncrement(), toSqlDate(body.getString(key)));
                        break;
                    case "datetime":
                    case "timestamp": // 转换为 java.sql.Timestamp 类型，根据具体的日期时间格式进行解析
                        ps.setTimestamp(i.getAndIncrement(), toTimestamp(body.getString(key)));
                        break;
                    case "time":
                        ps.setTime(i.getAndIncrement(), toSqlTime(body.getString(key)));
                        break;
                    case "long":
                    case "cblob":
                    case "text":
                        ps.setString(i.getAndIncrement(), body.getString(key));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type:" + fieldType);
                }
            } catch (Exception e) {
                throw new YondifRuntimeException(String.format("convert data type exception,key:{%s},fieldType:{%s}", key, fieldType), e);
            }
        });
    }

    private java.sql.Timestamp toTimestamp(String dateString) {
        return Timestamp.from(
                Instant.from(timestampFormatter.parse(dateString))
        );
    }

    private java.sql.Time toSqlTime(String dateString) {
        return java.sql.Time.valueOf(
                LocalTime.parse(dateString, timeFormatter)
        );
    }

    private java.sql.Date toSqlDate(String dateString) {
        return java.sql.Date.valueOf(
                LocalDate.parse(dateString, dateFormatter)
        );
    }
}
