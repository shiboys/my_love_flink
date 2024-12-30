package com.swj.sensors.flink_study.table_api_with_sql.table;

import com.swj.sensors.flink_study.table_api_with_sql.sources.GeneratorTableSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/25 20:24
 */
public class TableToDataStream {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(4000);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
    // watermark 生成会调用 periodic 生成器方法
    env.getConfig().setAutoWatermarkInterval(1000);

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.registerTableSource("table", new GeneratorTableSource(10, 100, 60, 0));

    Table table = tEnv.scan("table");

    DataStream<Row> appendStream = tEnv.toAppendStream(table, Row.class);
    appendStream.print();
    env.execute();
  }
}
