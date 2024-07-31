package com.yonyougov.yondif.job.dialect.dm.table;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;

public class DMDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:dm:");
    }

    @Override
    public JdbcDialect create() {
        return new DMDialect();
    }
}
