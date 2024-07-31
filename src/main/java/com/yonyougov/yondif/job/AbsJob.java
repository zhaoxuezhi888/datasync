package com.yonyougov.yondif.job;

import com.yonyougov.yondif.job.flatmap.BaseFlatMap;
import com.yonyougov.yondif.job.source.SourceFactory;
import com.yonyougov.yondif.job.source.kafka.KafkaSourceFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class AbsJob implements IJob {
    protected StreamExecutionEnvironment environment;
    protected ParameterTool parameterTool;
    protected BaseFlatMap baseFlatMap;

    /**
     * waiting...
     */
    public AbsJob(StreamExecutionEnvironment environment, ParameterTool parameterTool, BaseFlatMap baseFlatMap) {
        this.environment = environment;
        this.parameterTool = parameterTool;
        this.baseFlatMap = baseFlatMap;
    }

    @Override
    public void execute() throws Throwable {
        String jobName = parameterTool.get("job_name");
        //接收Kafka数据
        DataStream<ConsumerRecord<String, String>> source = getSource();
        //处理、发送数据
        process(source);
        //执行作业
        environment.execute(jobName);
    }

    public DataStream<ConsumerRecord<String, String>> getSource() throws Throwable {
        SourceFactory sourceFactory = new KafkaSourceFactory(environment, parameterTool);
        DataStream<ConsumerRecord<String, String>> dataStream = sourceFactory.createSource();
        return dataStream;
    }

    public abstract void process(DataStream<ConsumerRecord<String, String>> source) throws Throwable;
}
