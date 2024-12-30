package com.swj.sensors.flink_study.table_api_with_sql.sql;

import com.swj.sensors.flink_study.table_api_with_sql.sources.GeneratorTableSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/26 16:49
 * 主要演示 over window 的用法
 */
public class SqlDemo2 {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(4_000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
    env.getConfig().setAutoWatermarkInterval(1000);

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // 这次通过 TableSource 来注册，因为 TableSource 需要实现 DefinedRowTimeAttribute 接口，所以不需要向 DataStream 那样指定 timestamp

    tEnv.registerTableSource("input_table", new GeneratorTableSource(10, 10, 10, 10));
    Table inputTable = tEnv.scan("input_table");
    inputTable.printSchema();

    int overWindowSize = 10;
    String query = String.format("SELECT key, "
            + "rowtime, "
            + "COUNT(*) OVER (PARTITION BY key ORDER BY rowtime RANGE BETWEEN INTERVAL '%d' SECOND PRECEDING AND CURRENT ROW) AS cnt "
            + "FROM input_table"
        , overWindowSize);

    Table result = tEnv.sqlQuery(query);

    DataStream<Row> printStream = tEnv.toAppendStream(result, Types.ROW(Types.INT, Types.SQL_TIMESTAMP, Types.LONG));

    printStream.print();

    /**
     * 打印结果如下：
     *
     * 1,2023-12-26 10:33:55.058,98
     * 2,2023-12-26 10:33:55.058,98
     * 3,2023-12-26 10:33:55.058,98
     * 4,2023-12-26 10:33:55.058,98
     * 5,2023-12-26 10:33:55.058,98
     * 6,2023-12-26 10:33:55.058,98
     * 7,2023-12-26 10:33:55.058,98
     * 8,2023-12-26 10:33:55.058,98
     * 9,2023-12-26 10:33:55.058,98
     * 0,2023-12-26 10:33:55.058,98
     * 8,2023-12-26 10:33:55.163,98
     * 0,2023-12-26 10:33:55.163,98
     * 1,2023-12-26 10:33:55.163,98
     * 2,2023-12-26 10:33:55.163,98
     * 3,2023-12-26 10:33:55.163,98
     * 5,2023-12-26 10:33:55.163,98
     * 6,2023-12-26 10:33:55.163,98
     * 7,2023-12-26 10:33:55.163,98
     * 4,2023-12-26 10:33:55.163,98
     * 9,2023-12-26 10:33:55.163,98
     *
     * 10 秒的窗口，每秒 10 个批批次，每个批次 10 条记录，也就是0-9每条记录每秒的个数为 10 ，10 秒的窗口，就是100，因此在 over window 里面，
     * 每条记录的个数最终的结果结果将近 100
     */

    env.execute();
  }
}
