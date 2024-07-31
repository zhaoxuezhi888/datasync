package com.yonyougov.yondif.config.dboperator;

import com.yonyougov.yondif.exception.YondifRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class MyBatisFactory {
    private static final Logger logger = LogManager.getLogger(MyBatisFactory.class);
    private SqlSessionFactory sqlSessionFactory;
    private static MyBatisFactory instance;

    private MyBatisFactory() {

    }

    public static MyBatisFactory getInstance(String datasourceType) {
        if (instance == null) {
            instance = new MyBatisFactory();
            instance.createSqlSessionFactory(datasourceType);
        }
        return instance;
    }

    private void createSqlSessionFactory(String datasourceType) {
        try {
            String flinkConfDir = System.getProperty("conf.dir");
            logger.info("flink_conf_dir is {}", flinkConfDir);
            InputStream inputStream = null;
            switch (datasourceType) {
                case "mysql":
                    if (StringUtils.isNotBlank(flinkConfDir)) {
                        inputStream = new FileInputStream(flinkConfDir+"/mysql-config.xml");
                    }
                    else {
                        inputStream = Resources.getResourceAsStream("mysql-config.xml");
                    }
                    break;
                case "dm":
                    if (StringUtils.isNotBlank(flinkConfDir)) {
                        inputStream = new FileInputStream(flinkConfDir+"/dm-config.xml");
                    }
                    else {
                        inputStream = Resources.getResourceAsStream("dm-config.xml");
                    }
                    break;
                default:
                    break;
            }
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        } catch (IOException e) {
            throw new YondifRuntimeException("创建sqlSessionFactory出错", e);
        }
    }

    public static SqlSessionFactory getSqlSessionFactory(String datasourceType) {
        return MyBatisFactory.getInstance(datasourceType).sqlSessionFactory;
    }


}
