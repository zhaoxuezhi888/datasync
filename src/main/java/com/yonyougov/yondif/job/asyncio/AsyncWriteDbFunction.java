package com.yonyougov.yondif.job.asyncio;

import com.yonyougov.yondif.config.dboperator.MyBatisMapper;
import com.yonyougov.yondif.config.dboperator.MyBatisSession;
import com.yonyougov.yondif.job.flatmap.entity.AmsDatasyncExceptionMessage;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import com.yonyougov.yondif.log.LogMarker;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.ibatis.session.SqlSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsyncWriteDbFunction extends RichAsyncFunction<Message, Void> {
    private static final Logger logger = LogManager.getLogger(AsyncWriteDbFunction.class);
    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = new ThreadPoolExecutor(1,1,0L, TimeUnit.SECONDS,new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.DiscardPolicy());
    }

    @Override
    public void asyncInvoke(Message input, ResultFuture<Void> resultFuture) {
        executorService.submit(()->{
            //写入关系数据库
            SqlSession session = null;
            try {
                MyBatisSession myBatisSession = MyBatisMapper.getSession();
                AmsDatasyncExceptionMessage amsDatasyncExceptionMessage = new AmsDatasyncExceptionMessage();
                myBatisSession.getMapper().insert(amsDatasyncExceptionMessage);
                session = myBatisSession.getSession();
                if (resultFuture != null) {
                    // 标记异步请求完成
                    resultFuture.complete(new ArrayList<>());
                }
            } catch (Exception e) {
                logger.error(LogMarker.error_marker, "插入错误消息异常", e);
                if (resultFuture != null) {
                    // 标记异步请求完成
                    resultFuture.complete(new ArrayList<>());
                }
            } finally {
                if (session != null) {
                    session.close();
                }
            }
        });
    }

    @Override
    public void timeout(Message input, ResultFuture<Void> resultFuture) {
        logger.error(LogMarker.error_marker, "插入错误消息超时");
        if (resultFuture != null) {
            // 标记异步请求完成
            resultFuture.complete(new ArrayList<>());
        }
    }
}
