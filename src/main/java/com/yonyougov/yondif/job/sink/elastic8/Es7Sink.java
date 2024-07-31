package com.yonyougov.yondif.job.sink.elastic8;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.yonyougov.yondif.job.sink.SinkFactory;
import com.yonyougov.yondif.utils.EsUtils;
import com.yonyougov.yondif.utils.IntUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.util.List;

/**
 * @Author zxz
 * @description elastic search sink
 * @date 2022年11月03日 19:32
 */
@SuppressWarnings("unchecked")
public class Es7Sink<T, R> extends SinkFactory<R, String> {
    private static final Logger logger = LogManager.getLogger(Es7Sink.class);

    public Es7Sink(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        super(env, parameterTool);
    }

    @Override
    public R createSink(String... args) throws Exception {
        JSONObject sourceSetting;
        List<HttpHost> esAddresses;
        try {
            JSONObject setting = JSONObject.parseObject(parameterTool.get("source_" + parameterTool.getLong("dest_id")));
            sourceSetting = JSON.parseObject(setting.getString("source_setting"));
            esAddresses = EsUtils.getEsAddresses(sourceSetting.getString("elasticsearch.host"));
        } catch (MalformedURLException e) {
            throw new MalformedURLException("ES地址获取失败");
        }
        ElasticsearchEmitter emitter = new Es7Emitter(parameterTool);
        ElasticsearchSink<T> esBilder = new Elasticsearch7SinkBuilder()
                //#ES刷新前缓冲的最大动做量，es中action.queue.max_count：该参数定义了每个操作队列的最大操作数。默认值为1000
                .setBulkFlushMaxActions(IntUtils.getIntValue(sourceSetting.getIntValue("elasticsearch.bulk-flush.max-actions"), 1000))
                .setBulkFlushInterval(IntUtils.getLongValue(sourceSetting.getLongValue("elasticsearch.bulk-flush.interval"), 500L))
                //#ES刷新前缓冲区的最大数据大小（以兆字节为单位）
                .setBulkFlushMaxSizeMb(IntUtils.getIntValue(sourceSetting.getIntValue("elasticsearch.bulk-flush.max-size"), MemorySize.parse("10mb").getMebiBytes()))
                .setHosts(esAddresses.toArray(new HttpHost[0]))
                //#es请求超时，秒为单位
                .setConnectionRequestTimeout(IntUtils.getIntValue(sourceSetting.getIntValue("elasticsearch.connection.request-timeout"), 60000))
                //#es连接超时，秒为单位
                .setConnectionTimeout(IntUtils.getIntValue(sourceSetting.getIntValue("elasticsearch.connection.timeout"), 60000))
                //#essocket超时，秒为单位
                .setSocketTimeout(IntUtils.getIntValue(sourceSetting.getIntValue("elasticsearch.socket.timeout"), 60000))
                //重新添加由于资源受限（例如：队列容量已满）而失败的请求。对于其它类型的故障，例如文档格式错误，sink 将会失败。 如若未设置 BulkFlushBackoffStrategy (或者 FlushBackoffType.NONE)，那么任何类型的错误都会导致 sink 失败
                //这意味着如果有请求因为资源受限而失败，它们会被重新添加到队列中，然后再次发送。由于重新添加的请求会排在队列的末尾，所以可能会导致消息的顺序发生变化
                //可以通过修改以下两个参数来设置队列容量，action.queue.max_bytes：该参数定义了每个操作队列的最大字节数。默认值为100mb
                //action.queue.max_count：该参数定义了每个操作队列的最大操作数。默认值为1000,编辑elasticsearch.yml配置文件修改这两个值，默认为NONE，不要设置
                //.setBulkFlushBackoffStrategy(FlushBackoffType.CONSTANT,3,3000)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setEmitter(emitter)
                .setConnectionUsername(StringUtils.defaultIfBlank(sourceSetting.getString("elasticsearch.username"), ""))
                .setConnectionPassword(StringUtils.defaultIfBlank(sourceSetting.getString("elasticsearch.password"), ""))
                .build();
        logger.info("创建es7 sink完毕");
        return (R) esBilder;
    }
}
