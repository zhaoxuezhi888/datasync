package com.yonyougov.yondif.job.flatmap.splitstream;

import com.yonyougov.yondif.job.flatmap.entity.Message;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

public class SRSplitStream extends ProcessFunction<Message, String> {
    private final Map<String, OutputTag<String>> outputTagMap;

    public SRSplitStream(Map<String, OutputTag<String>> outputTagMap) {
        this.outputTagMap = outputTagMap;
    }
    
    @Override
    public void processElement(Message message, Context context, Collector<String> collector) throws Exception {
        String tableName = message.getTableName();
        context.output(outputTagMap.get(tableName), message.getBody().toJSONString());
    }
}
