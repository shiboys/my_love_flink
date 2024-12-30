package com.swj.sensors.flink_study.table_api_with_sql.windows;

import com.swj.sensors.flink_study.table_api_with_sql.sources.GeneratorTableSource;
import com.swj.sensors.flink_study.table_api_with_sql.sql.SqlDemo;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.PrintStream;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/25 21:31
 */
public class TumblingWindow {
  public static void main(String[] args) throws Exception {
    ParameterTool tool = ParameterTool.fromArgs(args);
    String outputPath = tool.get("outputPath", "/tmp/TumblingWindow");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(4_000);
    env.getConfig().setAutoWatermarkInterval(1000);

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(10)));

    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

    tableEnvironment.registerTableSource("table1", new GeneratorTableSource(10, 100, 60, 0));

    int tumblingWindowSize = 10;

    String querySql = String.format("SELECT "
        + "key, "
        + "rowtime, "
        + "COUNT(*) OVER (PARTITION BY key ORDER BY rowtime RANGE BETWEEN INTERVAL '%d' SECOND PRECEDING AND CURRENT ROW) AS cnt "
        + "FROM table1 ", tumblingWindowSize);

    Table queryTable = tableEnvironment.sqlQuery(querySql);

    // convert table into append-only data stream;
    DataStream<Row> appendStream =
        tableEnvironment.toAppendStream(queryTable, Types.ROW(Types.INT, Types.SQL_TIMESTAMP, Types.LONG));

    System.out.println("append stream begin to print:");
    appendStream.print();

    final StreamingFileSink<Row> sink =
        StreamingFileSink.forRowFormat(new Path(outputPath), (Encoder<Row>) (element, stream) -> {
          PrintStream printStream = new PrintStream(stream);
          System.out.println("sink one element: " + element.toString());
          printStream.println(element.toString());
          printStream.flush();
        })
            .withBucketAssigner(new SqlDemo.KeyBucketAssigner())
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build();

    appendStream
        // inject a KillMapper that forwards all records but terminates the first execution attempt
        // 将所有的记录向前推进(发送到下游算子），但是在第一次执行尝试之后停止。
        // 由于设置了重试策略，因此 Flink 会自动进行重试，但每次重试都是从
        .map(new SqlDemo.KillMapper())
        .addSink(sink).setParallelism(1);

    env.execute();
  }
}
