package com.yonyougov.yondif.job;

import com.yonyougov.yondif.job.flatmap.BaseFlatMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author zxz
 * @description
 * @date 2022年11月17日 15:35
 */
public class KafkaRowToMysqlJob extends AbsJob {
    protected StreamTableEnvironment tableEnv;

    public KafkaRowToMysqlJob(StreamExecutionEnvironment environment, ParameterTool parameterTool, BaseFlatMap baseFlatMap) {
        super(environment, parameterTool, baseFlatMap);
        tableEnv = StreamTableEnvironment.create(environment);
    }

    /**
     * 数据过滤转换及提交逻辑
     */
    @Override
    public void process(DataStream<ConsumerRecord<String, String>> source) {
        DataStream<Row> dataStream = source
                .flatMap(baseFlatMap)
                .name("数据转换")
                .uid("flatmap")
                .returns(
                        Types.ROW_NAMED(new String[]{"id", "name", "age"},
                                Types.INT, Types.STRING, Types.INT)
                );
        dataStream.print();

        //读取源
        Schema updateSchema = Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .primaryKey("id")
                .build();
        Table inputTable = tableEnv.fromChangelogStream(dataStream, updateSchema, ChangelogMode.upsert());
        tableEnv.createTemporaryView("InputTable", inputTable);

        String createTableDDL = "CREATE TEMPORARY TABLE ams_datasync_test (\n" +
                "  id INT NOT NULL,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://10.16.26.82:3306/yondif-ams-pcrw-online',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'Ufgov@12345',\n" +
                "   'table-name' = 'ams_datasync_test',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'sink.buffer-flush.interval' = '10s'\n" +
                ")";
        tableEnv.executeSql(createTableDDL);
        tableEnv.from("InputTable").insertInto("ams_datasync_test").execute();
        /*String insertIntoDDL = "INSERT INTO ams_datasync_test(id, name, age, pubts) SELECT id, name, age, pubts FROM InputTable";
        tableEnv.executeSql(insertIntoDDL);*/
    }
}
