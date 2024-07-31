package com.yonyougov.yondif.config.dboperator;

import com.yonyougov.yondif.mapper.BaseMapper;
import org.apache.ibatis.session.SqlSession;

import java.io.Closeable;
/*
* author:zxz
* data:2023-07-03
* */
public class MyBatisSession implements Closeable {
    private final SqlSession session;
    private final BaseMapper mapper;

    public MyBatisSession(SqlSession session, BaseMapper mapper) {
        this.session = session;
        this.mapper = mapper;
    }

    public BaseMapper getMapper() {
        return mapper;
    }

    public SqlSession getSession() {
        return session;
    }

    @Override
    public void close() {
        session.close();
    }
}
