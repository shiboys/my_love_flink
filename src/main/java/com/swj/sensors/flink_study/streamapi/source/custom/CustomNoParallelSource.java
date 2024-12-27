package com.swj.sensors.flink_study.streamapi.source.custom;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 14:43
 */
public class CustomNoParallelSource implements SourceFunction<Long> {

  volatile boolean isRunning = true;
  private long count;

  @Override
  public void run(SourceContext<Long> ctx) throws Exception {
    while (isRunning) {
      ctx.collect(++count);
      // 每隔 1 秒，生成一个数
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
