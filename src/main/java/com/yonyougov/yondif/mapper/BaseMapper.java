package com.yonyougov.yondif.mapper;

import com.yonyougov.yondif.job.flatmap.entity.AmsDatasyncExceptionMessage;
import org.apache.ibatis.annotations.Param;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author zxz
 */
public interface BaseMapper extends Serializable {
    /**
    * 根据jobId，读取job表相关信息
    */
    Map<String, Object> loadJobByJobId(@Param("jobId") String jobId);
    /**
     * 根据jobId，读取该job下的表
     */
    List<Map<String, Object>> loadTableByJobId(@Param("jobId") String jobId);

    /**
     * 根据jobId，读取该job下的表的字段映射
     */
    List<Map<String, Object>> loadTableFieldByJobId(@Param("jobId") String jobId);

    /**
     * 根据tableId，读取该table下的表的字段映射
     */
    List<Map<String, Object>> loadTableFieldByTableId(@Param("jobId") String tableId);
    /**
     * 根据sourceId获取数据源信息列表
     */
    List<Map<String, Object>> loadSourcesByJobId(@Param("ids") List<String> ids);

    /**
     * 插入异常消息表
     */
    Integer insert(AmsDatasyncExceptionMessage amsDatasyncExceptionMessage);
}
