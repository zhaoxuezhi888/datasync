package com.yonyougov.yondif.config.setting;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import com.yonyougov.yondif.config.dboperator.MyBatisMapper;
import com.yonyougov.yondif.config.dboperator.MyBatisSession;
import com.yonyougov.yondif.exception.YondifRuntimeException;
import com.yonyougov.yondif.job.dialect.IJdbcDialect;
import com.yonyougov.yondif.job.dialect.dm.datastream.DmJdbcDialect;
import com.yonyougov.yondif.job.dialect.mysql.MysqlJdbcDialect;
import com.yonyougov.yondif.log.LogMarker;
import com.yonyougov.yondif.mapper.BaseMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.*;

/**
 * @Author zxz
 * @description job类操作
 * @date 2022年11月17日 14:51
 */
public class JobService {
    private static final Logger logger = LogManager.getLogger(JobService.class);

    public static Map<String, String> loadJobByJobId(String jobId) throws Exception {
        Map<String, Object> jobOrigin;
        try (MyBatisSession myBatisSession = MyBatisMapper.getSession()) {
            BaseMapper baseMapper = myBatisSession.getMapper();
            jobOrigin = baseMapper.loadJobByJobId(jobId);
        }
        //根据jobid,读取数据库相关的配置
        if (null == jobOrigin || jobOrigin.isEmpty()) {
            logger.error(LogMarker.error_marker, "ams_datasync_job表不能为空");
            throw new YondifRuntimeException("ams_datasync_job表不能为空");
        }
        for (Map.Entry<String, Object> entry : jobOrigin.entrySet()) {
            if (entry.getValue() == null) {
                String errorMessage = String.format("ams_datasync_job表不能为空,job_id is %s,key is %s", jobId, entry.getKey());
                logger.error(LogMarker.error_marker, errorMessage);
                throw new YondifRuntimeException(errorMessage);
            }
        }
        logger.debug("加载ams_datasync_job结束,result:{}", jobOrigin);
        Map<String, String> job = Maps.transformValues(jobOrigin, Functions.toStringFunction());
        return job;
    }

    public static Map<String, String> loadTableByJobId(String jobId) throws Exception {
        Map<String, String> table = Maps.newHashMap();
        List<Map<String, Object>> tableOrigin;
        //根据jobid,读取数据库相关的配置
        try (MyBatisSession myBatisSession = MyBatisMapper.getSession()) {
            BaseMapper baseMapper = myBatisSession.getMapper();
            tableOrigin = baseMapper.loadTableByJobId(jobId);
        }

        if (null == tableOrigin || tableOrigin.isEmpty()) {
            logger.error(LogMarker.error_marker, "ams_datasync_table表不能为空");
            throw new YondifRuntimeException("ams_datasync_table表不能为空");
        }

        //循环list，检查table_name，index_name，database_name，primary_field均不能为空
        for (int i = 0; i < tableOrigin.size(); i++) {
            Map<String, Object> item = tableOrigin.get(i);
            for (Map.Entry<String, Object> entry : item.entrySet()) {
                if (!entry.getKey().equals("dest_database") && entry.getValue() == null) {
                    String errorMessage = String.format("ams_datasync_table表不能为空,table_id is %s,key is %s", item.get("table_id"), entry.getKey());
                    logger.error(LogMarker.error_marker, errorMessage);
                    throw new YondifRuntimeException(errorMessage);
                }
            }

            table.put(item.get("table_name").toString(), JSON.toJSONString(item));
            table.put(item.get("table_name").toString() + "_index", item.get("index_name").toString());
            table.put(item.get("table_name").toString() + "_database_name", item.get("database_name").toString());
            table.put(item.get("table_name").toString() + "_primary_field", item.get("primary_field").toString());
            table.put(item.get("table_name").toString() + "_es_primary_field", item.get("es_primary_field").toString());
            table.put(item.get("table_name").toString() + "_dest_database", String.valueOf(item.getOrDefault("dest_database", null)));
        }
        return table;
    }

    public static Map<String, String> loadTableListByJobId(String jobId) throws Exception {
        Map<String, String> jobList = Maps.newHashMap();
        //根据jobid,读取数据库相关的配置
        List<Map<String, Object>> tableOrigin;
        //根据jobid,读取数据库相关的配置
        try (MyBatisSession myBatisSession = MyBatisMapper.getSession()) {
            BaseMapper baseMapper = myBatisSession.getMapper();
            tableOrigin = baseMapper.loadTableByJobId(jobId);
        }

        if (null == tableOrigin || tableOrigin.isEmpty()) {
            logger.error(LogMarker.error_marker, "ams_datasync_table表不能为空");
            throw new YondifRuntimeException("ams_datasync_table表不能为空");
        }
        Map<String, String> tableMap = new HashMap<>();
        //循环list，检查table_name，index_name，database_name，primary_field均不能为空
        for (int i = 0; i < tableOrigin.size(); i++) {
            Map<String, Object> item = tableOrigin.get(i);
            for (Map.Entry<String, Object> entry : item.entrySet()) {
                if (!entry.getKey().equals("dest_database") && entry.getValue() == null) {
                    String errorMessage = String.format("ams_datasync_table表不能为空,table_id is %s,key is %s", item.get("table_id"), entry.getKey());
                    logger.error(LogMarker.error_marker, errorMessage);
                    throw new YondifRuntimeException(errorMessage);
                }
                tableMap.put(item.get("table_name").toString(), item.get("table_name").toString());
            }
        }
        jobList.put("table_list", JSON.toJSONString(tableMap));
        return jobList;
    }

    public static Map<String, String> loadTableFieldByJobId(String jobId) {
        Map<String, String> table = Maps.newHashMap();
        List<Map<String, Object>> tableIdList;
        try (MyBatisSession myBatisSession = MyBatisMapper.getSession()) {
            BaseMapper baseMapper = myBatisSession.getMapper();
            tableIdList = baseMapper.loadTableByJobId(jobId);
        }

        if (null == tableIdList || tableIdList.isEmpty()) {
            logger.error(LogMarker.error_marker, "ams_datasync_job_table表不能为空");
            throw new YondifRuntimeException("ams_datasync_job_table表不能为空");
        }
        for (int j = 0; j < tableIdList.size(); j++) {
            Map<String, Object> tableItem = tableIdList.get(j);
            String table_id = tableItem.get("table_id").toString();
            String table_name = String.valueOf(tableItem.get("table_name"));
            if (StringUtils.isBlank(table_name)) {
                String errorMessage = String.format("ams_datasync_table表不能为空,table_id is %s", table_id);
                logger.error(LogMarker.error_marker, errorMessage);
                throw new YondifRuntimeException(errorMessage);
            }
            List<Map<String, Object>> fieldList;
            try (MyBatisSession myBatisSession = MyBatisMapper.getSession()) {
                BaseMapper baseMapper = myBatisSession.getMapper();
                fieldList = baseMapper.loadTableFieldByTableId(table_id);
            }
            Map<String, String> fieldTemp = Maps.newHashMap();
            for (int i = 0; i < fieldList.size(); i++) {
                Map<String, Object> item = fieldList.get(i);
                if (MapUtils.isNotEmpty(item)) {
                    if (item.get("source_field") != null && item.get("dest_field") != null && !"".equals(item.get("source_field")) && !"".equals(item.get("dest_field"))) {
                        fieldTemp.put(String.valueOf(item.get("source_field")), String.valueOf(item.get("dest_field")));
                    } else {
                        String errorMessage = String.format("字段映射异常,field is null,table_name is %s", table_name);
                        logger.error(LogMarker.error_marker, errorMessage);
                        throw new YondifRuntimeException(errorMessage);
                    }
                } else {
                    String errorMessage = String.format("字段映射异常,mapping is null,table_name is %s", table_name);
                    logger.error(LogMarker.error_marker, errorMessage);
                    throw new YondifRuntimeException(errorMessage);
                }
            }
            table.put(table_name + "_field", JSON.toJSONString(fieldTemp));
        }
        return table;
    }

    public static Map<String, String> loadSourcesByJobId(ParameterTool parameterTool) {
        Map<String, String> table = Maps.newHashMap();

        //根据jobid,读取数据库相关的配置
        String mq_id = parameterTool.get("mq_id");
        String dest_id = parameterTool.get("dest_id");
        List<String> sourceIds = Arrays.asList(mq_id, dest_id);

        List<Map<String, Object>> sourceList;
        try (MyBatisSession myBatisSession = MyBatisMapper.getSession()) {
            BaseMapper baseMapper = myBatisSession.getMapper();
            sourceList = baseMapper.loadSourcesByJobId(sourceIds);
        }

        if (null == sourceList || sourceList.isEmpty()) {
            logger.error(LogMarker.error_marker, "ams_datasync_source表不能为空");
            throw new YondifRuntimeException("ams_datasync_source表不能为空");
        }

        for (int j = 0; j < sourceList.size(); j++) {
            Map<String, Object> sourceTemp = sourceList.get(j);
            String sourceId = sourceTemp.get("source_id").toString();
            Map<String, String> sourceItem = Maps.transformValues(sourceTemp, Functions.toStringFunction());
            table.put("source_" + sourceId, JSON.toJSONString(sourceItem));
        }
        return table;
    }

    public static ParameterTool loadTableMetaData(ParameterTool parameterTool) throws SQLException {
        JSONObject destSetting;
        try {
            JSONObject destSource = JSON.parseObject(parameterTool.get("source_" + parameterTool.getLong("dest_id")));
            int sourceType = destSource.getIntValue("source_type");
            if (sourceType != 0) {
                return parameterTool;
            }
            destSetting = JSON.parseObject(destSource.getString("source_setting"));
        } catch (Exception e) {
            throw new YondifRuntimeException("Sink目标地址获取失败");
        }
        String driverClassname = destSetting.getString("driverclassname");
        String hostName = destSetting.getString("hostname");
        String port = destSetting.getString("port");
        String userName = destSetting.getString("username");
        String password = destSetting.getString("password");

        //循环tablelist，找到对应的databasename
        JSONObject tableList = JSON.parseObject(parameterTool.get("table_list"));
        List<String> tableNames = new ArrayList<>();
        tableList.keySet().forEach(key -> {
            tableNames.add(tableList.getString(key));
        });
        for (String key : tableList.keySet()) {
            String tableName = tableList.getString(key);
            String databaseName = parameterTool.get(tableName + "_database_name");
            String url = null;
            IJdbcDialect jdbcDialect = null;
            if (driverClassname.contains("dm")) {
                url = "jdbc:dm://" + hostName + ":" + port + "/" + databaseName;
                jdbcDialect = new DmJdbcDialect();
            } else if (driverClassname.contains("mysql")) {
                url = "jdbc:mysql://" + hostName + ":" + port + "/" + databaseName;
                jdbcDialect = new MysqlJdbcDialect();
            }
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(url, userName, password);
                PreparedStatement stmt = conn.prepareStatement("select * from " + jdbcDialect.quoteIdentifier(tableName) + " limit 0");
                ResultSet rs = stmt.executeQuery();
                ResultSetMetaData meta = rs.getMetaData();
                Map<String, String> fieldTypeMap = Maps.newHashMap();
                Map<String, String> fieldType = Maps.newHashMap();
                int columnCount = meta.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    fieldTypeMap.put(meta.getColumnName(i), meta.getColumnTypeName(i));
                }
                rs.close();
                stmt.close();
                if (!fieldTypeMap.containsKey("is_deleted")) {
                    String errorMessage = String.format("is_deleted字段必须存在 %s", tableName);
                    logger.error(LogMarker.error_marker, errorMessage);
                    throw new YondifRuntimeException(errorMessage);
                }
                fieldType.put(tableName + "_field_type", JSON.toJSONString(fieldTypeMap));
                parameterTool = parameterTool.mergeWith(ParameterTool.fromMap(fieldType));
            } catch (SQLException throwables) {
                throw new YondifRuntimeException("连接数据库异常,{}", throwables);
            } finally {
                if (conn != null) {
                    conn.close();
                }
            }
        }
        return parameterTool;
    }
}
