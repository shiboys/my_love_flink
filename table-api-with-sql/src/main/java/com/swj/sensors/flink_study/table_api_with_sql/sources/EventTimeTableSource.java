package com.swj.sensors.flink_study.table_api_with_sql.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/24 16:15
 * DefinedRowtimeAttributes 抽取 rowTime 属性，就相当于 StreamDataSource 中自定义 TimeStamp 字段
 */
public class EventTimeTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes, DefinedFieldMapping {

  // 数据源的标签，可以是 flink sql 的 table name
  private final String sourceTag;

  // 每次发送多少keys
  private final int keysBatch;

  // 记录的 ms 偏移量
  private final int offsetSeconds;


  // 持续产生数据多长时间
  private final int durationSeconds;

  // 每批次产生之后，线程休眠的时间
  private final float rowsPerKeyAndSecond;

  public EventTimeTableSource(String sourceTag, int keysBatch, float rowsPerKeyAndSecond, int durationSeconds,
      int offsetSeconds) {
    this.sourceTag = sourceTag;
    this.keysBatch = keysBatch;
    this.durationSeconds = durationSeconds;
    this.offsetSeconds = offsetSeconds;
    this.rowsPerKeyAndSecond = rowsPerKeyAndSecond;
  }

  @Override
  public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
    return streamExecutionEnvironment.addSource(
        new DataGenerator(sourceTag, keysBatch, rowsPerKeyAndSecond, durationSeconds, offsetSeconds));
  }

  @Override
  public TableSchema getTableSchema() {
    return new TableSchema(
        new String[] {"key", "rowtime", "payload"},
        new TypeInformation<?>[] {Types.INT, Types.SQL_TIMESTAMP, Types.STRING}
    );
  }

  // 重新这个方法，就是重写 getProducedDataType,否则 flink 无法推断出数据的原始类型。
  @Override
  public TypeInformation<Row> getReturnType() {
    return Types.ROW(Types.INT, Types.LONG, Types.STRING);
  }

  @Override
  public String explainSource() {
    return "GeneratorTableSource";
  }

  @Override
  public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
    return Collections.singletonList(
        new RowtimeAttributeDescriptor("rowtime",
            new ExistingField("ts"),
            new BoundedOutOfOrderTimestamps(100) // 水位线时间 delay 100
        ));
  }

  /**
   * 这个接口提供了一个 从 DataSource<Row>  到 TableSchema 的映射
   *
   * @return
   */
  @Nullable
  @Override
  public Map<String, String> getFieldMapping() {
    Map<String, String> mapping = new HashMap<>();
    mapping.put("key", "f0");
    // 给 getRowtimeAttributeDescriptors 使用
    mapping.put("ts", "f1");
    mapping.put("payload", "f2");
    return mapping;
  }

  // ResultTypeQueryable 接口告知调用者产生的数据类型
  private static class DataGenerator implements SourceFunction<Row>, ResultTypeQueryable<Row>, ListCheckpointed<Long> {

    private long ms;

    // 数据源的标签，可以是 flink sql 的 table name
    private final String sourceTag;

    // 每次发送多少keys
    private final int keysBatch;

    // 记录的 ms 偏移量
    private final int offsetSeconds;


    // 持续产生数据多长时间
    private final int durationMs;

    // 每批次产生之后，线程休眠的时间
    private final int sleepMs;

    public DataGenerator(String sourceTag, int keysBatch, float rowsPerKeyAndSecond, int durationSeconds,
        int offsetSeconds) {
      this.sourceTag = sourceTag;
      this.keysBatch = keysBatch;
      this.durationMs = durationSeconds * 1000;
      this.offsetSeconds = offsetSeconds;

      // 产生一个批次的记录需要线程休眠的时长
      sleepMs = (int) (1000 / rowsPerKeyAndSecond);
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      long offsetMs = offsetSeconds * 2000L;
      while (ms < durationMs) {
        synchronized (ctx.getCheckpointLock()) {
          for (int i = 1; i <= keysBatch; i++) {
            Row row = Row.of(i + 1000, ms + offsetMs, sourceTag + " payload...");
            ctx.collect(row);
            System.out.println("Table Source: " + sourceTag + ", PayLoad: " + row);
          }
          ms += sleepMs;
        }
        // sleep 唤醒后继续抢锁
        Thread.sleep(sleepMs);
      }
    }

    @Override
    public void cancel() {

    }

    @Override
    public TypeInformation<Row> getProducedType() {
      return Types.ROW(Types.INT, Types.LONG, Types.STRING);
    }

    // 将 ms 持久化，应为 ms 是我们一个比较重要的逻辑判断
    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
      return Collections.singletonList(ms);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
      for (Long l : state) {
        ms += l;
      }
    }
  }
}
