package com.yonyougov.yondif.config.setting;

import com.yonyougov.yondif.Launcher;
import com.yonyougov.yondif.config.dboperator.MyBatisMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * @Author zxz
 * @description ParameterTool类操作
 * @date 2022年11月02日 9:31
 */
public class ParameterToolService {
    private static final Logger logger = LogManager.getLogger(ParameterToolService.class);

    /**
     * @description 创建ParameterTool实例, 根据jobid参数读取该jobid下的所有配置
     * @author zxz
     */
    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool
                .fromPropertiesFile(Launcher.class.getClassLoader().getResourceAsStream("application.properties"))
                .mergeWith(ParameterTool.fromArgs(args));
        // 获取 Flink 安装目录下的 conf 文件夹路径
        String flinkConfDir = System.getProperty("conf.dir");
        logger.info("flink_conf_dir is {}", flinkConfDir);
        // 拼接文件路径
        if (StringUtils.isNotBlank(flinkConfDir)) {
            parameterTool = parameterTool.mergeWith(ParameterTool.fromPropertiesFile(flinkConfDir + "/application.properties"));
        }
        MyBatisMapper.initializeMyBatisMapper(parameterTool.get("datasource.type"));
        parameterTool = getParameterTool(parameterTool);
        logger.info("成功加载完毕ParameterTool");
        return parameterTool;
    }

    public static ParameterTool getParameterTool(ParameterTool parameterTool) throws Exception {
        String jobId = parameterTool.get("job_id");
        Map<String, String> job = JobService.loadJobByJobId(jobId);
        parameterTool = parameterTool.mergeWith(ParameterTool.fromMap(job));
        Map<String, String> jobList = JobService.loadTableListByJobId(jobId);
        parameterTool = parameterTool.mergeWith(ParameterTool.fromMap(jobList));
        Map<String, String> table = JobService.loadTableByJobId(jobId);
        parameterTool = parameterTool.mergeWith(ParameterTool.fromMap(table));
        Map<String, String> tableField = JobService.loadTableFieldByJobId(jobId);
        parameterTool = parameterTool.mergeWith(ParameterTool.fromMap(tableField));
        Map<String, String> sourceField = JobService.loadSourcesByJobId(parameterTool);
        parameterTool = parameterTool.mergeWith(ParameterTool.fromMap(sourceField));
        parameterTool = JobService.loadTableMetaData(parameterTool);

        return parameterTool;
    }

}
