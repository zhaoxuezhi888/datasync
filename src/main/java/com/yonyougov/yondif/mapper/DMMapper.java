package com.yonyougov.yondif.mapper;

import com.yonyougov.yondif.job.flatmap.entity.AmsDatasyncExceptionMessage;
import org.apache.ibatis.annotations.*;

import java.util.List;
import java.util.Map;

/**
 * @author zxz
 * @description mysql操作
 * @date 2023-07-03
 */
@Mapper
public interface DMMapper extends BaseMapper {
    /**
     * 根据jobId，读取job表相关信息
     */
    @Override
    @Select("select a.job_id,a.job_name,a.source_id,a.mq_id,a.dest_id,b.job_class_path,b.env_class_path,b.flatmap_class_path,a.topic from ams_datasync_job a "
            +"inner join ams_datasync_job_class b on a.job_class_id=b.job_class_id where a.job_id=#{jobId}")
    Map<String, Object> loadJobByJobId(@Param("jobId") String jobId);

    /**
     * 根据jobId，读取该job下的表
     */
    @Override
    @Select("select c.table_id,c.table_name,c.index_name,c.dest_database,c.database_name," +
            "(select distinct source_field from ams_datasync_table_field where table_id = c.table_id and primary_field=1 limit 1) as primary_field, " +
            "(select distinct dest_field from ams_datasync_table_field where table_id = c.table_id and es_primary_field=1 limit 1) as es_primary_field " +
            "from ams_datasync_job a " +
            "inner join ams_datasync_job_table b on a.job_id=b.job_id " +
            "inner join ams_datasync_table c on b.table_id=c.table_id " +
            "where a.job_id=#{jobId}")
    List<Map<String, Object>> loadTableByJobId(@Param("jobId") String jobId);

    /**
     * 根据jobId，读取该job下的表的字段映射
     */
    @Override
    @Select("select c.table_id,c.table_name from ams_datasync_job a left join ams_datasync_job_table b on a.job_id=b.job_id " +
            "inner join ams_datasync_table c on b.table_id=c.table_id " +
            "where a.job_id=#{jobId}")
    List<Map<String, Object>> loadTableFieldByJobId(@Param("jobId") String jobId);

    /**
     * 根据tableId，读取该table下的表的字段映射
     */
    @Override
    @Select("select table_id,source_field,dest_field from ams_datasync_table_field where table_id=#{tableId} and is_delete=1")
    List<Map<String, Object>> loadTableFieldByTableId(@Param("tableId") String tableId);

    /**
     * 根据sourceId获取数据源信息列表
     */
    @Override
    @Select({
            "<script>",
            "select source_id,source_name,source_type,source_setting from ams_datasync_source where source_id in ",
            "<foreach item='item' collection='ids' open='(' separator=',' close=')'>",
            "#{item}",
            "</foreach>",
            "</script>"
    })
    List<Map<String, Object>> loadSourcesByJobId(@Param("ids") List<String> ids);

    /**
     * 插入异常消息表
     */
    @Override
    @Options(useGeneratedKeys=true,keyProperty="eventId")
    @Insert("insert into ams_datasync_exception_message" +
            " (trace_id,job_id,pk_id,error_code,database_name,table_name,index_name,message,exception,exception_status)" +
            " values(#{traceId},#{jobId},#{pkId},#{errorCode},#{databaseName},#{tableName},#{indexName},#{message},#{exception},#{exceptionStatus})")
    public Integer insert(AmsDatasyncExceptionMessage amsDatasyncExceptionMessage);
}
