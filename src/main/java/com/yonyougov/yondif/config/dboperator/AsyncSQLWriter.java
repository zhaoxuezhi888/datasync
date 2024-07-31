package com.yonyougov.yondif.config.dboperator;

import com.yonyougov.yondif.job.flatmap.entity.AmsDatasyncExceptionMessage;
import com.yonyougov.yondif.log.LogMarker;
import org.apache.ibatis.session.SqlSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

/**
 * 异常信息异步写入数据库中
 *
 * @Author zxz
 */
public class AsyncSQLWriter {
    private static final Logger logger = LogManager.getLogger(AsyncSQLWriter.class);

    public static void writeDataAsync(AmsDatasyncExceptionMessage amsDatasyncExceptionMessage) {
        CompletableFuture.runAsync(() -> {
            SqlSession session = null;
            try {
                MyBatisSession myBatisSession = MyBatisMapper.getSession();
                myBatisSession.getMapper().insert(amsDatasyncExceptionMessage);
                session = myBatisSession.getSession();
            } catch (Exception e) {
                logger.error(LogMarker.error_marker, "插入错误消息异常", e);
            } finally {
                if (session != null) {
                    session.close();
                }
            }
        });
    }
}
