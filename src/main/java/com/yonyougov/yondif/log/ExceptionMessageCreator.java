package com.yonyougov.yondif.log;

import com.yonyougov.yondif.job.flatmap.entity.AmsDatasyncExceptionMessage;
import org.apache.logging.log4j.ThreadContext;

import java.util.Date;

public class ExceptionMessageCreator {
    public static AmsDatasyncExceptionMessage create(String errorCode, String message, String exception) {
        AmsDatasyncExceptionMessage amsDatasyncExceptionMessage = new AmsDatasyncExceptionMessage();
        amsDatasyncExceptionMessage.setTraceId(ThreadContext.get(LogField.TRACE_ID));
        amsDatasyncExceptionMessage.setJobId(ThreadContext.get(LogField.JOB_ID));
        amsDatasyncExceptionMessage.setPkId(ThreadContext.get(LogField.PK_ID));
        amsDatasyncExceptionMessage.setErrorCode(errorCode);
        amsDatasyncExceptionMessage.setDatabaseName(ThreadContext.get(LogField.DATABASE_NAME));
        amsDatasyncExceptionMessage.setTableName(ThreadContext.get(LogField.TABLE_NAME));
        amsDatasyncExceptionMessage.setIndexName(ThreadContext.get(LogField.INDEX_NAME));
        amsDatasyncExceptionMessage.setMessage(message);
        amsDatasyncExceptionMessage.setException(exception);
        amsDatasyncExceptionMessage.setExceptionStatus(0);
        amsDatasyncExceptionMessage.setPubts(new Date());
        return amsDatasyncExceptionMessage;
    }
}
