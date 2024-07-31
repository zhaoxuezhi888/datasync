package com.yonyougov.yondif.job.source.kafka;

import com.yonyougov.yondif.log.LogField;
import com.yonyougov.yondif.log.LogMarker;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.io.UnsupportedEncodingException;
import java.util.Optional;
import java.util.UUID;

/**
 * @Author zxz
 * 自定义kafka反序列化类
 * @date 2022年07月11日 13:46
 */
public class KafkaDeserialization implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {
    private static final Logger logger = LogManager.getLogger(KafkaDeserialization.class);
    private static final long serialVersionUID = 1L;
    private final String encoding = "UTF8";
    private ParameterTool parameterTool;

    public KafkaDeserialization(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) {
        String value = null;
        try {
            if (record == null || record.value() == null) {
                return null;
            }
            value = new String(record.value(), encoding);
            String traceId = UUID.randomUUID().toString();
            ThreadContext.clearMap();

            ThreadContext.put(LogField.TRACE_ID, traceId);
            ThreadContext.put(LogField.JOB_ID, parameterTool.get("job_id"));
            ConsumerRecord consumerRecord = new ConsumerRecord(record.topic(),
                    record.partition(),
                    record.offset(),
                    System.currentTimeMillis(),
                    TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                    traceId,
                    value,
                    new RecordHeaders(),
                    Optional.empty());

            return consumerRecord;
        } catch (UnsupportedEncodingException e) {
            logger.error(LogMarker.error_marker, value, e);
        }
        return null;
    }

    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
        });
        //return getForClass(String.class);
    }
}