package com.swj.sensors.flink_study.table_api_with_sql.windows;

import static org.apache.flink.table.api.Expressions.$;

import com.swj.sensors.flink_study.table_api_with_sql.sources.OrderSourceFunction;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/26 11:24
 */
public class Windows {


  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(4_000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
    env.getConfig().setAutoWatermarkInterval(1000);

    DataStreamSource<Tuple4<Long, String, Integer, Long>> orderSourceStream =
        env.addSource(new OrderSourceFunction(10, 0.1f, 60));

    // 给 sourceSteam 指定 timestamp, 用于 table api 的各种 window 统计
    // 有关 table api 的各种窗口统计，请参考 note.md 中有关 Flink Sql 中有关窗口的描述
    SingleOutputStreamOperator<Tuple4<Long, String, Integer, Long>> orderSourceWithTs =
        orderSourceStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.forGenerator((context -> new PeriodicWatermarkGenerator()))
                .withTimestampAssigner((context -> new TimestampExtractor())));

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // 可以通过 TableSource，也可以通过 tEnv.registerDataStream(DataStream)

    // 为了向更高版本的 API 靠拢，这里不再使用 这个 API  registerDataStream(String name, DataStream<T> dataStream, String fields);
    // 而是使用 expression 表达式，这个能更精确每个字段
    // createTemporaryView 这个是抄官方 1.11 文档的，省去了 先 fromDataStream 的到 table，然后再注册 table 的麻烦
    tEnv.createTemporaryView("Orders", orderSourceWithTs, $("user"), $("product"), $("amount"),
        $("event_time").rowtime(),
        $("process_time").proctime());
    // tEnv.registerTable("Orders", orderTable);

    // 在 event_time 上执行 滚动窗口。刚开始提交报错，
    // Call to auxiliary group function 'TUMBLE_START' must have matching call to group function '$TUMBLE' in GROUP BY clause
    // 我看了下，我写了 group by tumble ，后来一刹那，发现 里面的 Interval 时间间隔不一致，坑爹呀
    String sql = "SELECT "
        + "user, "
        + "TUMBLE_START(event_time, INTERVAL '20' SECOND) AS tStart, "
        + "SUM(amount) AS CNT "
        + "FROM Orders "
        + "GROUP BY TUMBLE(event_time, INTERVAL '20' SECOND), user";

    //Table t1 = tEnv.sqlQuery(sql);

    // 在 process_time 上 执行 1 天的滚动窗口查询
    sql = "SELECT user , TUMBLE_START(process_time, INTERVAL '1' DAY) AS wSTART, SUM(amount) AS cnt "
        + "FROM Orders GROUP BY TUMBLE(process_time, INTERVAL '1' DAY),user ";
    //Table t1 = tEnv.sqlQuery(sql);

    // 在 event_time 上执行 窗口总大小为 1 分钟，滑动大小为 10 秒钟的查询
    sql =
        "SELECT product, HOP_START(event_time, INTERVAL '10' SECOND, INTERVAL '1' MINUTE) as hStart,SUM(amount) AS cnt "
            + "FROM Orders GROUP BY product, HOP(event_time, INTERVAL '10' SECOND, INTERVAL '1' MINUTE)";
    //Table t1 = tEnv.sqlQuery(sql);

    // 在 event_time 上执行 窗口大小为 12 小时的 session window 查询
    sql = "SELECT user, "
        + "SESSION_START(event_time,INTERVAL '12' HOUR) AS sStart, "
        + "SESSION_ROWTIME(event_time,INTERVAL '12' HOUR) AS sEnd, "
        + "SUM(amount) AS cnt "
        + "FROM Orders GROUP BY user, SESSION(event_time, INTERVAL '12' HOUR)";
    Table t1 = tEnv.sqlQuery(sql);

    // DataStream<Row> result = tEnv.toAppendStream(t1, Types.ROW(Types.LONG, Types.SQL_TIMESTAMP, Types.INT));
    DataStream<Row> result =
        tEnv.toAppendStream(t1, Types.ROW(Types.LONG, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.INT));

    result.print();

    env.execute();
  }


  private static class TimestampExtractor implements TimestampAssigner<Tuple4<Long, String, Integer, Long>> {

    @Override
    public long extractTimestamp(Tuple4<Long, String, Integer, Long> element, long recordTimestamp) {
      return element.f3;
    }
  }


  private static class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple4<Long, String, Integer, Long>> {

    private long currentMaxTs = Long.MIN_VALUE;

    @Override
    public void onEvent(Tuple4<Long, String, Integer, Long> event, long eventTimestamp, WatermarkOutput output) {
      currentMaxTs = Math.max(currentMaxTs, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      // watermark 的生成比 ts 要晚上 100 毫秒
      long ts = currentMaxTs == Long.MIN_VALUE ? Long.MIN_VALUE : currentMaxTs - 100;
      output.emitWatermark(new Watermark(ts));
    }
  }

}
