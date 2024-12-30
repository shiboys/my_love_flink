package com.swj.sensors.flink_study;

import ch.qos.logback.core.encoder.EchoEncoder;
import com.swj.sensors.flink_study.serializer.CdcTableSerializer;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
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

        env.enableCheckpointing(300_000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://localhost:8020/flink-cdc-simple1/checkpoints"));
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);


        MySqlSource<String> cdcSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("canal")
                .password("canal")
                .databaseList("metadata")
                .tableList("metadata.sf_push_callback")
                .startupOptions(StartupOptions.initial())
                //.deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new CdcTableSerializer())
                .serverTimeZone("Asia/Shanghai")
                .build();

        DataStreamSource<String> mysqlStreamSource = env.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        mysqlStreamSource.print();

        // timezone 设置不正确，也报错，坑爹

        env.execute("Flink MySql Connector CDC With Java");
    }


}