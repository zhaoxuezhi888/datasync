package com.yonyougov.yondif.job.flatmap.splitstream;

import com.yonyougov.yondif.job.flatmap.entity.Message;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MessageSplitStream extends ProcessFunction<Message, Message> {
    private final OutputTag<Message> errorOutputTag;

    public MessageSplitStream(OutputTag<Message> errorOutputTag) {
        this.errorOutputTag = errorOutputTag;
    }
    
    @Override
    public void processElement(Message message, Context context, Collector<Message> collector) throws Exception {
        if(message.isError()){
            context.output(errorOutputTag, message);
            return;
        }
        collector.collect(message);
    }
}
