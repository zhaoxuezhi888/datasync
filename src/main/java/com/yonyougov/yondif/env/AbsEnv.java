package com.yonyougov.yondif.env;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class AbsEnv {
    protected ParameterTool parameterTool;

    public AbsEnv(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    public abstract StreamExecutionEnvironment createEnvironment();
}
