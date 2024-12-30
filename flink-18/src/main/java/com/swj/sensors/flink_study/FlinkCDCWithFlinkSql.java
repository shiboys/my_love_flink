package com.swj.sensors.flink_study;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2024/12/27 21:58
 */
public class FlinkCDCWithFlinkSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE sf_push_callback_binlog (\n" +
                "     id INT,\n" +
                "     channel_type STRING,\n" +
                "     url STRING,\n" +
                "     offical_url STRING,\n" +
                "     create_time TIMESTAMP(0),\n" +
                "     PRIMARY KEY(id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'localhost',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'canal',\n" +
                "     'password' = 'canal',\n" +
                "     'database-name' = 'metadata',\n" +
                "     'table-name' = 'sf_push_callback');");
        Table table = tableEnv.sqlQuery("select * from sf_push_callback_binlog");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        env.execute("flink cdc with flink sql");
    }
}
