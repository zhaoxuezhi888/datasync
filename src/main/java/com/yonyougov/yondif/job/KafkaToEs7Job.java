package com.yonyougov.yondif.job;

import com.yonyougov.yondif.job.asyncio.AsyncWriteDbFunction;
import com.yonyougov.yondif.job.flatmap.BaseFlatMap;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import com.yonyougov.yondif.job.flatmap.splitstream.MessageSplitStream;
import com.yonyougov.yondif.job.sink.elastic8.Es7Sink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.TimeUnit;

/**
 * @Author zxz
 * @description
 * @date 2022年11月17日 15:35
 */
public class KafkaToEs7Job extends AbsJob {

    public KafkaToEs7Job(StreamExecutionEnvironment environment, ParameterTool parameterTool, BaseFlatMap baseFlatMap) {
        super(environment, parameterTool, baseFlatMap);
    }

    /**
     * 数据过滤转换及提交逻辑
     */
    @Override
    public void process(DataStream<ConsumerRecord<String, String>> source) throws Throwable {
        DataStream<Message> dataStream = source
                .flatMap(baseFlatMap)
                .name("数据转换")
                .uid("flatmap");
        //侧输出错误消息
        OutputTag<Message> errorOutputTag = new OutputTag("errormessage", TypeInformation.of(Message.class));
        SingleOutputStreamOperator<Message> dataStreamFilter = dataStream.process(new MessageSplitStream(errorOutputTag));
        SideOutputDataStream<Message> errorStream = dataStreamFilter.getSideOutput(errorOutputTag);
        AsyncDataStream.unorderedWait(errorStream,new AsyncWriteDbFunction(),30, TimeUnit.SECONDS);

        Sink<Message> dsgSchemaSink = new Es7Sink<Message, Sink<Message>>(environment, parameterTool).createSink();
        dataStreamFilter.sinkTo(dsgSchemaSink).uid("es7sink").name("提交信息到ES");
    }
}
