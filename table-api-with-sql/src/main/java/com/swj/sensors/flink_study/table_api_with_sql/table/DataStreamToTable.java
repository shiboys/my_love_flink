package com.swj.sensors.flink_study.table_api_with_sql.table;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/25 14:50
 */
public class DataStreamToTable {

  private static final int ONE_HOUR_MILLIS = 3600_000;

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(4000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
    // watermark 生成会调用 periodic 生成器方法
    env.getConfig().setAutoWatermarkInterval(1000);

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Tuple3<Integer, Long, String>> inputStream =
        env.addSource(new DataGenerator("tableStream", 5, 100f, 60, 2));
    //inputStream.print();
    // 根据 note.md 文档和官网的描述 .rowtime() 可以表明将该字段作为事件时间。
    // 这里就是官网说的第二种情况，在 DataStream 转换成 Table 的时候转换。
    // job提交运行之后，用 long 作为 rowtime 会非常慢？ 这是为什么？
    // 破案了，在 flink 1.18 环境运行的话，会提示没有设置 timestamp 字段，然后有看了看官方的文档
    // 在 DataStream to Table 的转换时，inputStream要调用 inputStream.assignTimestampsAndWatermarks(...);
    // 调用之后再设置 rowtime() 就可以了。之前只所以慢，是因为 flink 没有找到 timestamp 字段导致一直在查找，所以很慢
    // 而 .proctime() 则作为处理时间，且只能为附加的逻辑字段作为最后一个字段

    SingleOutputStreamOperator<Tuple3<Integer, Long, String>> inputStreamWithTs =
        inputStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.forGenerator((ctx) -> new PeriodicalWatermarkGenerator()).withTimestampAssigner(
                (ctx) -> new TimestampExtractor()));
    Table table = tEnv.fromDataStream(inputStreamWithTs,
        $("key")
        , $("event_time").rowtime()
        , $("pay_load")
        , $("process_time").proctime()
    );
    // 将 table 根据窗口进行划分
    GroupWindowedTable tableWindow = table.window(Tumble
        .over(lit(10).second())
        .on($("process_time"))
        .as("userActionWindow")
    );
    // 上面这个 window 感觉没用到
    DataStream<Row> dataStream =
        tEnv.toAppendStream(table,
            Types.ROW(Types.INT, Types.SQL_TIMESTAMP, Types.STRING, Types.SQL_TIMESTAMP));
    System.out.println("print table rows:");
    dataStream.print();
    env.execute();
  }

  //
  private static class DataGenerator implements SourceFunction<Tuple3<Integer, Long, String>> {

    private final String sourceTag;

    // 每次产生多少条记录
    private final int batchSize;
    // 生成 TimeStamp 偏移量时间
    private final int offsetSecond;

    // 总共生成多长时间的数据
    private final int durationMs;
    // 每次产生数据之后的休眠时间
    private final int sleepMs;

    private long ms = 0l;

    public DataGenerator(String sourceTag, int batchSize, float rowsPerKeyAndSecond, int durationSeconds,
        int offsetSecond) {
      this.sourceTag = sourceTag;
      this.batchSize = batchSize;
      this.offsetSecond = offsetSecond;
      this.durationMs = durationSeconds * 1000;
      this.sleepMs = (int) (1000 / rowsPerKeyAndSecond);
    }



    @Override
    public void run(SourceContext<Tuple3<Integer, Long, String>> ctx) throws Exception {
      long offsetMs = offsetSecond * 2000L;
      //long offsetMs = System.currentTimeMillis() - ONE_HOUR_MILLIS;

      while (ms < durationMs) {
        synchronized (ctx.getCheckpointLock()) {
          for (int i = 0; i < batchSize; i++) {
            Tuple3<Integer, Long, String> row = Tuple3.of(i + 1000, ms + offsetMs, sourceTag + " payload...");
            //System.out.println("Data Stream To Table Source: " + sourceTag + ", payload:" + row);
            ctx.collect(row);
          }
          ms += sleepMs;
        }
        Thread.sleep(sleepMs);
      }
    }

    @Override
    public void cancel() {

    }


//    @Override
//    public TypeInformation<Tuple3<Integer,Long,String>> getProducedType() {
//      return Types.ROW(Types.INT, Types.LONG, Types.STRING);
//    }
  }


  private static class TimestampExtractor implements TimestampAssigner<Tuple3<Integer, Long, String>> {

    @Override
    public long extractTimestamp(Tuple3<Integer, Long, String> tuple3, long l) {
      return tuple3.f1;
    }
  }


  private static class PeriodicalWatermarkGenerator
      implements WatermarkGenerator<Tuple3<Integer, Long, String>>, Serializable {

    private long currentTimestamp = Long.MIN_VALUE;

    /**
     * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者可以基于事件本身生成 watermark
     */
    @Override
    public void onEvent(Tuple3<Integer, Long, String> tuple3, long eventTimestamp, WatermarkOutput watermarkOutput) {
      currentTimestamp = Math.max(currentTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
      long availableWatermark = currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1;
      watermarkOutput.emitWatermark(new Watermark(availableWatermark));
    }
  }
}
