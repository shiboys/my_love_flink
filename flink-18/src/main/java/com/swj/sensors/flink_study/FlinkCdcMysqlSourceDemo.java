package com.swj.sensors.flink_study;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2024/12/09 18:03
 */
public class FlinkCdcMysqlSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> cdcSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("canal")
                .password("canal")
                .databaseList("metadata")
                .tableList("metadata.sf_push_callback")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .build();

        DataStreamSource<String> mysqlStreamSource = env.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        mysqlStreamSource.print();

        // timezone 设置不正确，也报错，坑爹

        env.execute("Flink MySql Connector CDC With Java");
    }


}