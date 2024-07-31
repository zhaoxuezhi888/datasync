package com.yonyougov.yondif.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.yonyougov.yondif.job.flatmap.BaseFlatMap;
import com.yonyougov.yondif.job.flatmap.entity.Message;
import com.yonyougov.yondif.job.flatmap.splitstream.DMSplitStream;
import com.yonyougov.yondif.job.sink.jdbc.BatchJdbcSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author zxz
 * @description
 * @date 2022年11月17日 15:35
 */
public class KafkaToMysqlJob extends AbsJob {

    public KafkaToMysqlJob(StreamExecutionEnvironment environment, ParameterTool parameterTool, BaseFlatMap baseFlatMap) {
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
        //将数据流中的数据分发到不同的侧输出流中，每个侧输出流对应一个表
        Map<String, OutputTag<Message>> outputTagMap = new HashMap<>();
        JSONObject tableList = JSON.parseObject(parameterTool.get("table_list"));
        List<String> tableNames = new ArrayList<>();
        tableList.keySet().forEach(key -> {
            tableNames.add(tableList.getString(key));
        });
        for (String key : tableList.keySet()) {
            String tableName = tableList.getString(key);
            OutputTag<Message> tableTag = new OutputTag(tableName, TypeInformation.of(Message.class));
            outputTagMap.put(tableName, tableTag);
        }
        SingleOutputStreamOperator<Message> splitDataStream = dataStream.process(new DMSplitStream(outputTagMap));

        //为每个表创建单独的 JdbcSink，将相应表的数据插入到数据库中
        Map<String, SinkFunction<Message>> jdbcSinks = new BatchJdbcSink(environment, parameterTool, "mysql").createSink(tableNames.stream().collect(Collectors.joining(",")));
        //将每个侧输出流中的数据插入到相应的表中
        for (String key : tableList.keySet()) {
            String tableName = tableList.getString(key);
            splitDataStream.getSideOutput(outputTagMap.get(tableName)).addSink(jdbcSinks.get(tableName)).uid("sink_" + tableName);
        }
    }
}
