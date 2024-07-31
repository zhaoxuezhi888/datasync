package com.yonyougov.yondif.job.source.kafka;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.yonyougov.yondif.job.source.SourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @Author zxz
 * @description
 * @date 2022年11月17日 16:25
 */
public class KafkaSourceFactory extends SourceFactory {
    private static final Logger logger = LogManager.getLogger(KafkaSourceFactory.class);

    public KafkaSourceFactory(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        super(env, parameterTool);
    }

    @Override
    public DataStream<ConsumerRecord<String, String>> createSource() {
        JSONObject mqSetting = JSON.parseObject(parameterTool.get("source_" + parameterTool.getLong("mq_id")));
        JSONObject sourceSetting = JSON.parseObject(mqSetting.getString("source_setting"));
        //0 基础数据kafka 1业务数据kafka
        String topicName = parameterTool.get("topic");
        String groupId = parameterTool.get("job_id");
        //默认从最后位点消费，如果想从最早offset消费，需要手动传group_id和earliest参数
        OffsetsInitializer offsetsInitializer;
        switch (parameterTool.get("offset_type", "")) {
            case "earliest":
                // 从最早位点开始消费
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case "time":
                // 从时间戳大于等于指定时间戳（毫秒）的数据开始消费
                offsetsInitializer = OffsetsInitializer.timestamp(parameterTool.getLong("start_time"));
                break;
            case "latest":
                // 从最末尾位点开始消费
                offsetsInitializer = OffsetsInitializer.latest();
                break;
            default:
                // 从消费组提交的位点开始消费，如果提交位点不存在，从已提交的偏移量开始消费
                offsetsInitializer = OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
                break;
        }
        //本地调试用groupId
        if (StringUtils.isNotBlank(parameterTool.get("group_id"))) {
            groupId = parameterTool.get("group_id");
        }

        Source kafkaSource = KafkaSource.<ConsumerRecord<String, String>>builder()
                .setBootstrapServers(sourceSetting.getString("kafka.bootstrap.servers"))
                .setTopics(topicName)
                .setGroupId(topicName + "_" + groupId)
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserialization(parameterTool)))
                .build();
        SingleOutputStreamOperator<ConsumerRecord<String, String>> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), topicName)
                .setParallelism(env.getParallelism())
                .uid("kafkaSource")
                .name("接收数据流");
        logger.info("创建kafka source factory 成功");
        return dataStreamSource;
    }
}
