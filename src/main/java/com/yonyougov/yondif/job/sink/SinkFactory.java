package com.yonyougov.yondif.job.sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author zxz
 * @description
 * @date 2022年11月17日 16:13
 */
public abstract class SinkFactory<R, P> {
    protected StreamExecutionEnvironment env;
    protected ParameterTool parameterTool;

    protected SinkFactory(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        this.env = env;
        this.parameterTool = parameterTool;
    }

    /**
     * @return DataStream
     * @description Build the source data flow
     * @author zxz
     */
    public abstract R createSink(P... args) throws Exception;
}
