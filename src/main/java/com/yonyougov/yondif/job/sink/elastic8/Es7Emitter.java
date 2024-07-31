package com.yonyougov.yondif.job.sink.elastic8;

import com.yonyougov.yondif.job.flatmap.entity.Message;
import com.yonyougov.yondif.log.LogMarker;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.RequestIndexer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.Date;

/**
 * @Author zxz
 * @description 自定义es sink逻辑
 * @description tuple9<traceId, dml_type, primary_key, primary_value, database_name, table_name, index_name, mof_div_code, body>
 * @date 2022年07月05日 14:39
 */
public class Es7Emitter implements ElasticsearchEmitter<Message> {
    private static final Logger logger = LogManager.getLogger(Es7Emitter.class);
    private ParameterTool parameterTool;

    public Es7Emitter(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    @Override
    public void emit(Message input, SinkWriter.Context context, RequestIndexer indexer) {
        switch (input.getDmlType()) {
            case INSERT:
            case UPDATE:
                processUpsert(input, indexer);
                break;
            case DELETE:
                processDelete(input, indexer);
                break;
            default:
                break;
        }
    }

    /**
     * @description 使用带有确定性id的UpdateRequests和upsert方法，当为连接器配置AT_LEAST_ONCE交付时，可以在Elasticsearch中实现恰好一次的语义。
     * @description tuple9<traceId, dml_type, primary_key, primary_value, database_name, table_name, index_name, mof_div_code, body>
     * @description 按照区划进行route, 如果不存在，则看是否是动态索引，如果不是，则按年度路由，最后都不是，按id进行route
     */
    private void processUpsert(Message input, RequestIndexer indexer) {
        try {
            input.getBody().put("opTime",input.getOpTime());
            input.getBody().put("inKafkaTime",input.getInKafkaTime());
            input.getBody().put("inFlinkTime",input.getInFlinkTime());
            FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
            String dateString = df.format(new Date());
            input.getBody().put("inEsTime",dateString);
            UpdateRequest updateRequest = (new UpdateRequest(input.getIndexName(), input.getPrimaryValue()))
                    .docAsUpsert(true)
                    .doc(input.getBody())
                    .retryOnConflict(10)
                    .waitForActiveShards(ActiveShardCount.ALL);                    ;
            if (parameterTool.getInt("trace_log", 0) == 1) {
                logger.info(LogMarker.trace_marker, input.getBody());
            }
            indexer.add(updateRequest);
        } catch (Exception e) {
            logger.error(LogMarker.error_marker, input.toString(), e);
        }
    }

    private void processDelete(Message input, RequestIndexer indexer) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest(input.getIndexName(), input.getPrimaryValue())
                    .waitForActiveShards(ActiveShardCount.ALL);
            indexer.add(deleteRequest);
            if (parameterTool.getInt("trace_log", 0) == 1) {
                logger.info(LogMarker.trace_marker, input.getBody());
            }
        } catch (Exception e) {
            logger.error(LogMarker.error_marker, input.toString(), e);
        }
    }

    /*
        算子关闭时，清理日志的threadcontext
    */
    @Override
    public void close() {
        ThreadContext.clearAll();
    }
}