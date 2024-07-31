package com.yonyougov.yondif.config.dboperator;

import com.yonyougov.yondif.mapper.DMMapper;
import com.yonyougov.yondif.mapper.BaseMapper;
import com.yonyougov.yondif.mapper.MysqlMapper;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;

public class MyBatisMapper {
    private String datasourceType;

    private MyBatisMapper(String datasourceType) {
        this.datasourceType = datasourceType;

    }

    private static class SingletonHolder {
        private static MyBatisMapper INSTANCE;

        private static void initialize(String datasourceType) {
            INSTANCE = new MyBatisMapper(datasourceType);
        }
    }

    public static void initializeMyBatisMapper(String datasourceType) {
        SingletonHolder.initialize(datasourceType);
    }

    public static MyBatisMapper getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public static MyBatisSession getSession() {
        SqlSession session = MyBatisFactory.getSqlSessionFactory(SingletonHolder.INSTANCE.datasourceType).openSession(ExecutorType.REUSE, true);
        BaseMapper myBatisMapper = null;
        switch (SingletonHolder.INSTANCE.datasourceType) {
            case "mysql":
                myBatisMapper = session.getMapper(MysqlMapper.class);
                break;
            case "dm":
                myBatisMapper = session.getMapper(DMMapper.class);
                break;
            default:
                break;
        }
        return new MyBatisSession(session, myBatisMapper);
    }
}
