package com.swj.sensors.flink_study.statebackend.statemachine.generataor;

import com.swj.sensors.flink_study.statebackend.statemachine.event.Event;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/12 12:59
 * 供 Flink 使用的 Source。
 * 该Source 既可以作为 flink-sink-kafka-producer 的  Source
 * 也可以作为 flink-source-kafka-consumer 的一种替代.
 * 该 Source 继承自 RichParallelSourceFunction，实现 run 方法和 cancel 方法
 * run 方法的主要逻辑是，生成一个 EventGenerator， 根据 当前运行的子任务数，和 当前子任务的 index ，获取 generator 负责的 ip 段
 * 在该 ip 段下模拟产生 event 数据，并发送到下游
 */

@SuppressWarnings("serial")
public class EventGeneratorSource extends RichParallelSourceFunction<Event> {

  private volatile boolean isRunning = true;
  private EventGenerator eventGenerator;
  private long delayPerRecordMills;

  public EventGeneratorSource(double errorProbability, long delayPerRecordMills) {
    Preconditions.checkArgument(errorProbability >= 0.0 && errorProbability <= 1.0,
        "error probability muse be in [0.0,1.1]");
    Preconditions.checkArgument(delayPerRecordMills >= 0, "delay per record in mills must be >= 0");
    eventGenerator = new EventGenerator(errorProbability);
    this.delayPerRecordMills = delayPerRecordMills;
  }

  @Override
  public void run(SourceContext<Event> ctx) throws Exception {
    int tasks = getRuntimeContext().getNumberOfParallelSubtasks();
    int stepSize = Integer.MAX_VALUE / tasks;
    int min = stepSize * getRuntimeContext().getIndexOfThisSubtask();
    int max = min + stepSize;
    while (isRunning) {
      Event event = eventGenerator.next(min, max);
      ctx.collect(event);
      if (delayPerRecordMills > 0) {
        Thread.sleep(delayPerRecordMills);
      }
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
