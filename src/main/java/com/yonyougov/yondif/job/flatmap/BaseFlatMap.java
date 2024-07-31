package com.yonyougov.yondif.job.flatmap;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

/**
 * @Author zxz
 * @description 对接收的kafka的数据进行必须的过滤和验证
 * @return
 * @date 2022年07月23日 13:28
 */
public abstract class BaseFlatMap<T> extends RichFlatMapFunction<ConsumerRecord<String, String>, T> {
    private static final Logger logger = LogManager.getLogger(BaseFlatMap.class);
    private static final long serialVersionUID = 1L;
    protected ParameterTool parameterTool;
    protected JSONObject tableList;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger.info("BaseFlatMap open");
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        tableList = JSONObject.parseObject(parameterTool.get("table_list"));
    }

    /*
        算子关闭时，清理日志的threadcontext
    */
    @Override
    public void close() {
        ThreadContext.clearAll();
        parameterTool = null;
    }
}
