package com.yonyougov.yondif.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.yonyougov.yondif.job.asyncio.AsyncWriteDbFunction;
import com.yonyougov.yondif.job.flatmap.BaseFlatMap;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import com.yonyougov.yondif.job.flatmap.splitstream.MessageSplitStream;
import com.yonyougov.yondif.job.flatmap.splitstream.SRSplitStream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author zxz
 * @description
 * @date 2022年11月17日 15:35
 */
public class KafkaToSRJob extends AbsJob {

    public KafkaToSRJob(StreamExecutionEnvironment environment, ParameterTool parameterTool, BaseFlatMap baseFlatMap) {
        super(environment, parameterTool, baseFlatMap);
    }

    /**
     * 数据过滤转换及提交逻辑
     */
    @Override
    public void process(DataStream<ConsumerRecord<String, String>> source) {
        DataStream<Message> dataStream = source
                .flatMap(baseFlatMap)
                .name("数据转换")
                .uid("flatmap");
        //侧输出错误消息
        OutputTag<Message> errorOutputTag = new OutputTag("errormessage", TypeInformation.of(Message.class));
        SingleOutputStreamOperator<Message> dataStreamFilter = dataStream.process(new MessageSplitStream(errorOutputTag));
        SideOutputDataStream<Message> errorStream = dataStreamFilter.getSideOutput(errorOutputTag);
        AsyncDataStream.unorderedWait(errorStream,new AsyncWriteDbFunction(),3, TimeUnit.SECONDS);
        //将数据流中的数据分发到不同的侧输出流中，每个侧输出流对应一个表
        Map<String, OutputTag<String>> outputTagMap = new HashMap<>();
        JSONObject tableList = JSON.parseObject(parameterTool.get("table_list"));
        for (String key : tableList.keySet()) {
            String tableName = tableList.getString(key);
            OutputTag<String> tableTag = new OutputTag(tableName, TypeInformation.of(String.class));
            outputTagMap.put(tableName, tableTag);
        }
        SingleOutputStreamOperator<String> splitDataStream = dataStreamFilter.process(new SRSplitStream(outputTagMap));
        Map<String, SinkFunction<String>> srSinks = new HashMap<>();
        JSONObject destSource = JSON.parseObject(parameterTool.get("source_" + parameterTool.getLong("dest_id")));
        JSONObject destSetting = JSON.parseObject(destSource.getString("source_setting"));
        for (String key : tableList.keySet()) {
            String tableName = tableList.getString(key);
            String fields = JSONObject.parseObject(parameterTool.get(tableName + "_field")).values()
                    .stream()
                    .map(Object::toString)
                    .reduce("", (a, b) -> a + "," + b);
            if (!fields.isEmpty()) {
                fields = fields.substring(1);
            }
            SinkFunction<String> srSink = StarRocksSink.sink(
                    StarRocksSinkOptions.builder()
                            .withProperty("jdbc-url", "jdbc:mysql://" + destSetting.getString("hostname") + ":" + destSetting.getString("tcpport") + "/" + parameterTool.get(tableName + "_database_name"))
                            .withProperty("load-url", destSetting.getString("hostname") + ":" + destSetting.getString("httpport"))
                            .withProperty("username", destSetting.getString("username"))
                            .withProperty("password", destSetting.getString("password"))
                            .withProperty("table-name", tableName)
                            .withProperty("database-name", parameterTool.get(tableName + "_database_name"))
                            .withProperty("sink.properties.partial_update", "true")
                            .withProperty("sink.properties.columns", fields + ",__op")
                            .withProperty("sink.properties.format", "json")
                            .withProperty("sink.properties.strip_outer_array", "true")
                            .withProperty("sink.buffer-flush.interval-ms", "1000")
                            .withProperty("sink.max-retries", "3")
                            .withProperty("sink.semantic", "at-least-once")
                            .withProperty("sink.properties.max_filter_ratio", "0")
                            .withProperty("sink.properties.strict_mode", "true")
                            .build()
            );
            srSinks.put(tableName, srSink);
        }

        //将每个侧输出流中的数据插入到相应的表中
        for (String key : tableList.keySet()) {
            String tableName = tableList.getString(key);
            splitDataStream.getSideOutput(outputTagMap.get(tableName)).addSink(srSinks.get(tableName)).uid("sink_" + tableName);
        }
    }
}
