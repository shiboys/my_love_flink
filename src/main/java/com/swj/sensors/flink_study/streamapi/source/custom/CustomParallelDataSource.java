package com.swj.sensors.flink_study.streamapi.source.custom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 15:09
 */
public class CustomParallelDataSource implements ParallelSourceFunction<Tuple2<String, Long>> {

  volatile boolean isRunning = true;
  private long count = 0;

  @Override
  public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
    while (isRunning) {
      // 每秒产生一个数，String 为 当前线程名称。
      // 因为我本机是 Standalone 安装的，不支持多并行度，所以就不测试了
      // 数据源支持多并行度，需要 实现 ParallelSourceFunction
      ctx.collect(new Tuple2<>(Thread.currentThread().getName(), ++count));
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning=false;
  }
}
