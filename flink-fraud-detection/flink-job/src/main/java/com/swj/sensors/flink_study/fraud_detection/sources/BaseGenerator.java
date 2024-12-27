package com.swj.sensors.flink_study.fraud_detection.sources;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;

import java.util.SplittableRandom;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/14 12:33
 * 元素生成器的基类
 */
public abstract class BaseGenerator<T> extends RichParallelSourceFunction<T> implements CheckpointedFunction {
  // 每秒最大记录数
  protected int maxRecordsPerSecond;

  private volatile boolean isRunning = true;

  private long id = -1;

  private ListState<Long> idState;

  protected BaseGenerator() {
    this.maxRecordsPerSecond = -1;
  }

  protected BaseGenerator(int maxRecordsPerSecond) {
    Preconditions.checkArgument(maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
        "maxRecordsPerSecond must be -1 or greater than 0");
    this.maxRecordsPerSecond = maxRecordsPerSecond;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    if (id == -1) { // 没有被 state 初始化(比如从 checkpoint 重启)
      id = getRuntimeContext().getIndexOfThisSubtask();
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    idState.clear();
    idState.add(id);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    idState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<Long>("ids",
        BasicTypeInfo.LONG_TYPE_INFO));
    if (context.isRestored()) {
      long max = Long.MIN_VALUE;
      for (Long l : idState.get()) {
        max = Math.max(max, l);
      }
      id = max + getRuntimeContext().getIndexOfThisSubtask();
    }
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    final SplittableRandom random = new SplittableRandom();
    final int numberOfParallelSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();
    final Throttler throttler = new Throttler(maxRecordsPerSecond, numberOfParallelSubTasks);
    final Object snapshotLock = ctx.getCheckpointLock();

    while (isRunning) {

      T record = randomGenerate(random, id);
      synchronized (snapshotLock) { // 加这个锁，是为了保证发送数据的线程安全 ？
        if (record != null) {
          // 产生 record 元素
          ctx.collect(record);
        }
        id += numberOfParallelSubTasks;
      }
      // 执行限流
      throttler.throttle();
    }
  }

  public abstract T randomGenerate(SplittableRandom random, long id);

  @Override
  public void cancel() {
    isRunning = false;
  }

  public int getMaxRecordsPerSecond() {
    return maxRecordsPerSecond;
  }
}
