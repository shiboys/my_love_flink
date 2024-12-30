package com.swj.sensors.flink_study.table_api_with_sql.sources;

import lombok.extern.slf4j.Slf4j;
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
 * @since 2023/12/25 20:26
 * 一个专门用来生成 Table 的 StreamSource
 */
public class GeneratorTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes, DefinedFieldMapping {

  private final int batchSize;
  private final int durationSeconds;
  private final float rowsPerKeyAndSecond;
  private final int offsetSeconds;

  public GeneratorTableSource(int batchSize, float rowsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
    this.batchSize = batchSize;
    this.offsetSeconds = offsetSeconds;
    this.durationSeconds = durationSeconds;
    this.rowsPerKeyAndSecond = rowsPerKeyAndSecond;
  }

  @Override
  public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
    return execEnv.addSource(new Generator(batchSize, rowsPerKeyAndSecond, durationSeconds, offsetSeconds));
  }

  @Override
  public TypeInformation<Row> getReturnType() {
    // 这个方法仍然是接收的 dataSource 的字段类型
    return Types.ROW(Types.INT, Types.LONG, Types.STRING);
  }

  // table schema 就返回了从 stream 转变成 table 的字段类型。第二个字段就从 long 类型变成 sql_timestamp 类型。
  @Override
  public TableSchema getTableSchema() {
    return new TableSchema(
        new String[] {"key", "rowtime", "payload"},
        new TypeInformation<?>[] {Types.INT, Types.SQL_TIMESTAMP, Types.STRING}
    );
  }

  @Override
  public String explainSource() {
    return "GeneratorTableSource";
  }

  // 返回一个 RowTime Descriptor 用来描述 event time 怎么生成
  @Override
  public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
    return Collections.singletonList(
        new RowtimeAttributeDescriptor("rowtime",
            new ExistingField("ts"),
            new BoundedOutOfOrderTimestamps(100)
        )
    );
  }

  @Nullable
  @Override
  public Map<String, String> getFieldMapping() {
    Map<String, String> mapping = new HashMap<>();
    mapping.put("key", "f0");
    mapping.put("ts", "f1");
    mapping.put("payload", "f2");
    return mapping;
  }

  @Slf4j
  private static class Generator implements SourceFunction<Row>, ResultTypeQueryable<Row>, ListCheckpointed<Long> {

    private final int batchSize;
    private final int durationMs;
    private final int sleepMs;
    private final int offsetSeconds;

    private long ms = 0L;

    public Generator(int batchSize, float rowsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
      this.batchSize = batchSize;
      this.durationMs = durationSeconds * 1000;
      this.offsetSeconds = offsetSeconds;
      this.sleepMs = (int) (1000 / rowsPerKeyAndSecond);
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      long offsetMs = offsetSeconds * 2000L;
      while (ms < durationMs) {
        synchronized (ctx.getCheckpointLock()) {
          for (int i = 0; i < batchSize; i++) {
            Row row = Row.of(i, System.currentTimeMillis(), "Some payload...");
            log.info(row.toString());
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

    @Override
    public TypeInformation<Row> getProducedType() {
      return Types.ROW(Types.INT, Types.LONG, Types.STRING);
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
      return Collections.singletonList(ms);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
      long ll = 0L;
      for (Long l : state) {
        ll += l;
      }
      System.out.println("ms value restored from state is " + ll);
      ms += ll;
    }
  }

}
