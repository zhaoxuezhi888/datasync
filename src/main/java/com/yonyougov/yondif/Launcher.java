package com.yonyougov.yondif;

import com.yonyougov.yondif.config.setting.ParameterToolService;
import com.yonyougov.yondif.env.AbsEnv;
import com.yonyougov.yondif.exception.YondifRuntimeException;
import com.yonyougov.yondif.job.IJob;
import com.yonyougov.yondif.job.flatmap.BaseFlatMap;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

/**
 * @Author zxz
 * @description 数据库同步的Flink DataStream作业主入口
 * @date 2022年10月28日 13:45
 */
public class Launcher {
    private static final Logger logger = LogManager.getLogger(Launcher.class);

    public static void main(String[] args) {
        ParameterTool parameterTool;
        try {
            Arrays.stream(args).forEach(arg -> logger.info("读取参数:{}", arg));
            //获取job参数
            parameterTool = ParameterToolService.createParameterTool(args);

            //反射获取env
            AbsEnv env = (AbsEnv) ConstructorUtils.invokeConstructor(Class.forName(parameterTool.get("env_class_path")), parameterTool);

            logger.info("AbsEnv实例加载完成,执行类路径为:{}", parameterTool.get("env_class_path"));
            StreamExecutionEnvironment environment = env.createEnvironment();
            environment.getConfig().setGlobalJobParameters(parameterTool);

            environment.setParallelism(1);
            //反射获取flatmap
            BaseFlatMap baseFlatMap = (BaseFlatMap) ConstructorUtils.invokeConstructor(Class.forName(parameterTool.get("flatmap_class_path")));
            logger.info("baseFlatMap实例加载完成,执行类路径为:{}", parameterTool.get("flatmap_class_path"));
            //反射并执行相应的job
            IJob job = (IJob) ConstructorUtils.invokeConstructor(Class.forName(parameterTool.get("job_class_path")), environment, parameterTool, baseFlatMap);
            logger.info("job_class_path实例加载完成,执行类路径为:{}", parameterTool.get("job_class_path"));
            job.execute();
        } catch (Throwable throwable) {
            throw new YondifRuntimeException("作业启动失败", throwable);
        }
        logger.info("作业名111222:{}，初始化成功", parameterTool.get("job_name"));
    }
}
