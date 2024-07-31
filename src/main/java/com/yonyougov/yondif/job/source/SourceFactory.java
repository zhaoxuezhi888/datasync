package com.yonyougov.yondif.job.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author zxz
 * @description
 * @date 2022年11月17日 16:13
 */
public abstract class SourceFactory {
    protected StreamExecutionEnvironment env;
    protected ParameterTool parameterTool;

    protected SourceFactory(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        this.env = env;
        this.parameterTool = parameterTool;
    }

    /**
     * @return DataStream
     * @description Build the source data flow
     * @author zxz
     */
    public abstract DataStream<ConsumerRecord<String, String>> createSource() throws Exception;
}
