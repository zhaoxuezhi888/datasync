package com.yonyougov.yondif.job.flatmap.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @description ams_datasync_exceptionmessage
 * @author zxz
 * @date 2023-04-06
 */
@Data
public class AmsDatasyncExceptionMessage implements Serializable {

    private static final long serialVersionUID = -3583412182156687319L;
    /**
     * 唯一id
     */
    private int eventId;

    /**
     * 跟踪id
     */
    private String traceId;

    /**
     * 任务启动id
     */
    private String jobId;

    /**
     * 主键id
     */
    private String pkId;

    /**
     * 错误码
     */
    private String errorCode;

    /**
     * 数据库名
     */
    private String databaseName;

    /**
     * 表名r
     */
    private String tableName;

    /**
     * 对应的索引名
     */
    private String indexName;

    /**
     * 异常消息内容
     */
    private String message;

    /**
     * 错误消息
     */
    private String exception;

    /**
     * 异常处理状态 0未处理 1 已处理r
     */
    private int exceptionStatus;

    /**
     * 插入时间
     */
    private Date pubts;

    public AmsDatasyncExceptionMessage() {}
}