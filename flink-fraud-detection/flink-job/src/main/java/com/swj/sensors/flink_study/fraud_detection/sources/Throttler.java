package com.swj.sensors.flink_study.fraud_detection.sources;

import org.apache.flink.util.Preconditions;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/14 14:54
 * 限流器 (单位时间内流入多少元素，超出的流量不允许)
 * 当前的实现中，不允许是通过 Thread.sleep 来让线程休眠实现的
 * 至于 sleep 的大小，主要是通过 当前时间-消耗的时间的剩余时间来算
 */
public class Throttler {
  // 限流大小
  private int throttleBatchSize;
  // 当前元素数
  private int currentRecordsSize;

  // 限流的时间单位，以纳秒计算
  private long nanosPerBatch;

  // 下一个批次的开始时间
  private long startOfNextBatch;

  /**
   * @param recordsPerSeconds 限流大小，以秒计算
   * @param numberOfSubTasks  flink 集群中的子任务数
   */
  public Throttler(int recordsPerSeconds, int numberOfSubTasks) {
    Preconditions.checkArgument(recordsPerSeconds == -1 || recordsPerSeconds > 0,
        "recordsPerSecond must be positive or -1 (infinite)");
    Preconditions.checkArgument(numberOfSubTasks > 0,
        "number of numberOfSubTasks must be greater than 0");
    if (recordsPerSeconds == -1) {
      throttleBatchSize = -1;
      currentRecordsSize = 0;
      nanosPerBatch = 0;
      startOfNextBatch = System.nanoTime() + nanosPerBatch;
      return;
    }

    int recordsPerSubTask = recordsPerSeconds / numberOfSubTasks;

    // 超过 1 万每秒的流量，说明流量比较大，那限流需要更精确
    if (recordsPerSubTask > 10_000) {
      // 这里精确到每 20 微秒计算一次，那每秒的数字转化成每 20ms 就是除以 500
      throttleBatchSize = recordsPerSubTask / 500;
      // 2ms
      nanosPerBatch = 20_000_000L;
    } else {
      // 小于 1 万的流量, 将流量分成 20 份
      throttleBatchSize = recordsPerSubTask / 20 + 1;
      // 每份流量的持续时间计算方式如下：tbatch/subtaskbatch * 1s 表示的纳秒数。
      nanosPerBatch = ((int) (1_000_000_000L / recordsPerSubTask)) * throttleBatchSize;
    }
    startOfNextBatch = System.nanoTime() + nanosPerBatch;
    this.currentRecordsSize = 0;
  }

  /**
   * 执行限流
   */
  public void throttle() throws InterruptedException {
    if (throttleBatchSize == -1) { // 不限流
      return;
    }
    if (++currentRecordsSize < throttleBatchSize) { // 不到限流量
      return;
    }
    // 到了限流量，开始限流，recordSize 计数器清零
    currentRecordsSize = 0;
    long now = System.nanoTime();
    //计算休眠时间
    long remainingMs = (startOfNextBatch - now) / 1_000_000L;
    if (remainingMs > 0) { // 说明时间还没到
      startOfNextBatch += nanosPerBatch;
      Thread.sleep(remainingMs);
    } else {
      // 1 毫秒之内
      startOfNextBatch = now + nanosPerBatch;
    }
  }

}
