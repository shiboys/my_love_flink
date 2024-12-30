package com.swj.sensors.flink_study.table_api_with_sql.sql;

import com.swj.sensors.flink_study.table_api_with_sql.sources.EventTimeTableSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/24 18:24
 */
public class EventTimeStreamSQL {

  public static void main(String[] args) throws Exception {
    ParameterTool tool = ParameterTool.fromArgs(args);
    String outputPath = tool.get("outputPath", "/tmp/EventTimeStreamSQL.txt");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(4_000);
    env.getConfig().setAutoWatermarkInterval(1000);

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

    // 注意这里是 registerTableSource 而不是 registerTable
    // table1 的记录每次产生 5 个，id 都是 1001:1005 之间，适合 over window
    tableEnvironment.registerTableSource("table1", new EventTimeTableSource("table1", 5, 0.2f, 60, 0));
    // table2 的 rowtime 字段 有 offsetSecond 秒的延迟
    tableEnvironment.registerTableSource("table2", new EventTimeTableSource("table2", 1, 0.2f, 60, 5));

    int overWindowSizeSeconds = 5;
    // table2 适合做 滚动窗口
    int tumblingWindowSizeSeconds = 10;
    String overQuery = String.format("SELECT"
        + " key,"
        + " rowtime,"
        + " COUNT(*) OVER (PARTITION BY key ORDER BY rowtime RANGE BETWEEN INTERVAL '%d' SECOND PRECEDING AND CURRENT ROW) AS cnt"
        + " FROM table1 ", overWindowSizeSeconds);

    //showWindowTableStream(tableEnvironment, overWindowSizeSeconds);
    //tumblingTableWindow(tableEnvironment, tumblingWindowSizeSeconds);

    // 在 overQuery 的基础上，增加 tumbling 窗口, 把 t1 聚合成 10 秒钟的滚动窗口
    String tumblingSl = String.format("SELECT "
            + "key, "
            + "CASE SUM(cnt)/COUNT(*) WHEN 2 THEN 1 ELSE 0 END AS correct, "
            + "TUMBLE_START(rowtime, INTERVAL '%d' SECOND) AS wStart, "
            + "TUMBLE_ROWTIME(rowtime, INTERVAL '%d' SECOND) AS rowtime, "
            + "TUMBLE_END(rowtime, INTERVAL '%d' SECOND) AS wEnd "
            + "FROM (%s) t "
            + "WHERE rowtime > timestamp '1970-01-01 00:00:01' "
            + "GROUP BY key,TUMBLE(rowtime,INTERVAL '%d' SECOND) ",
        tumblingWindowSizeSeconds,
        tumblingWindowSizeSeconds,
        tumblingWindowSizeSeconds,
        overQuery,
        tumblingWindowSizeSeconds);

    String joinQuery = String.format(
        "SELECT "
            + "t1.key, "
            + "t2.correct, "
            + "t2.rowtime as rowtime, "
            + "t2.wStart "
            + "FROM table2 t1, (%s) t2 "
            + "WHERE t1.key = t2.key AND "
            + "t1.rowtime BETWEEN t2.rowtime AND t2.rowtime + INTERVAL '%d' SECOND ",
        tumblingSl, tumblingWindowSizeSeconds
    );

    System.out.println("JoinQuery :" + joinQuery);

    String aggregateQuery = String.format("SELECT "
            + "SUM(correct) AS correct, "
            + "TUMBLE_START(rowtime, INTERVAL '20' SECOND) AS rowtime "
            + "FROM (%s) t "
            + "GROUP BY TUMBLE(rowtime, INTERVAL '20' SECOND) "
        , joinQuery);


    System.out.println("final aggregate sql: " + aggregateQuery);
    Table aggregateTable = tableEnvironment.sqlQuery(aggregateQuery);
    DataStream<Row> aggregateStream =
        tableEnvironment.toAppendStream(aggregateTable, Types.ROW(Types.INT, Types.SQL_TIMESTAMP));
    aggregateStream.print();

    env.execute();
  }

  private static void tumblingTableWindow(StreamTableEnvironment tableEnvironment, int tumblingWindowSizeSeconds) {
    String tumblingSql = String.format("SELECT "
            + "key, "
            + "COUNT(*) AS count_num, "
            + "TUMBLE_START(rowtime, INTERVAL '%d' SECOND) AS wStart, "
            + "TUMBLE_ROWTIME(rowtime, INTERVAL '%d' SECOND) AS eventTime, "
            + "TUMBLE_END(rowtime, INTERVAL '%d' SECOND) AS wEnd "
            + "FROM table2 "
            + "WHERE rowtime > TIMESTAMP '1970-01-01 00:00:01' "
            + "GROUP BY key, TUMBLE(rowtime, INTERVAL '%d' SECOND)"
        , tumblingWindowSizeSeconds
        , tumblingWindowSizeSeconds
        , tumblingWindowSizeSeconds
        , tumblingWindowSizeSeconds
    );

    System.out.println("tumble query : " + tumblingSql);

    Table table = tableEnvironment.sqlQuery(tumblingSql);
    DataStream<Row> tumblingResultStream = tableEnvironment.toAppendStream(table,
        Types.ROW(Types.INT, Types.LONG, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP));

    tumblingResultStream.print();
  }

  private static void showWindowTableStream(StreamTableEnvironment tableEnvironment,
      int overWindowSizeSeconds, String overQuery, boolean printable) {
    // 窗口查询

    System.out.println("OVERQUERY:" + overQuery);
    Table table = tableEnvironment.sqlQuery(overQuery);

    // 这里的类型一定不能出错
    DataStream<Row> overWindowStream =
        tableEnvironment.toAppendStream(table, Types.ROW(Types.INT, Types.SQL_TIMESTAMP, Types.LONG));
    if (printable) {
      overWindowStream.print();
    }
  }
}
