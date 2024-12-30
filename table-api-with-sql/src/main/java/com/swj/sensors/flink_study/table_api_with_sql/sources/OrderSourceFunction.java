package com.swj.sensors.flink_study.table_api_with_sql.sources;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/26 11:25
 * 就是一个订单的 SourceFunction
 * 订单的 4 个字段/属性，用 Tuple4 表示
 * int orderId, string product, int productCount, long timestamp
 */
public class OrderSourceFunction
    implements SourceFunction<Tuple4<Long, String, Integer, Long>>, ListCheckpointed<Long> {

  private static final String[] PRODUCTS = {"Gucci", "Louis Vuitton", "Chanel", "YSL"};

  private static final Random random = new Random();

  private final int batchSize;

  private final int durationMs;

  private final int sleepMs;

  private long ms = 0L;

  // 仍然是 generator 那一套

  public OrderSourceFunction(int batchSize, float rowsPerKeyAndSecond, int durationSeconds) {
    this.batchSize = batchSize;
    this.durationMs = durationSeconds * 1000;
    this.sleepMs = (int) (1000 / rowsPerKeyAndSecond);
  }



  @Override
  public void run(SourceContext<Tuple4<Long, String, Integer, Long>> ctx) throws Exception {
    int pSize = PRODUCTS.length;
    while (ms < durationMs) {
      synchronized (ctx.getCheckpointLock()) {
        for (int i = 0; i < batchSize; i++) {
          ctx.collect(
              Tuple4.of((long) i, PRODUCTS[random.nextInt(pSize)], random.nextInt(100), System.currentTimeMillis()));
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
