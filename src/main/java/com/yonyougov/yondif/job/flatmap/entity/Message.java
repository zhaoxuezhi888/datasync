package com.yonyougov.yondif.job.flatmap.entity;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class Message implements Serializable {
    private static final long serialVersionUID = 809964615016775563L;
    /*
    * 消息唯一跟踪ID
    */
    private String tarceId;

    /*
     * 消息类型
     */
    private DmlType dmlType;

    /*
     * 消息主键
     */
    private String primaryKey;

    /*
     * 消息主键值
     */
    private String primaryValue;

    /*
     * 源库名
     */
    private String databaseName;

    /*
     * 目标库名
     */
    private String destDatabase;

    /*
     * 表名
     */
    private String tableName;

    /*
     * 索引名
     */
    private String indexName;

    /*
     * 数据生成时间
     */
    private String opTime;

    /*
     * 数据生成时间
     */
    private String inKafkaTime;

    /*
     * 数据入flink时间
     */
    private String inFlinkTime;

    /*
     * 是否错误消息
     */
    private boolean isError;
    /**
     * 错误码
     */
    private String errorCode;
    /**
     * 异常消息内容
     */
    private String message;

    /**
     * 错误消息
     */
    private String exception;
    /*
     * 消息体
     */
    private JSONObject body;
}
