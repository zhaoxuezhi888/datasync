package com.yonyougov.yondif.job.flatmap.splitstream;

import com.yonyougov.yondif.job.flatmap.entity.Message;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

public class DMSplitStream extends ProcessFunction<Message, Message> {
    private final Map<String, OutputTag<Message>> outputTagMap;

    public DMSplitStream(Map<String, OutputTag<Message>> outputTagMap) {
        this.outputTagMap = outputTagMap;
    }
    
    @Override
    public void processElement(Message message, Context context, Collector<Message> collector) throws Exception {
        String tableName = message.getTableName();
        context.output(outputTagMap.get(tableName), message);
    }
}
