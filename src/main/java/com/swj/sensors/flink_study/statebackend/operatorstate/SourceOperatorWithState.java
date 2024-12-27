package com.swj.sensors.flink_study.statebackend.operatorstate;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Collections;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/08 20:50
 * 带状图的数据源比其他算子需要注意更多东西。为了保证更新状态以及输出的原子性（用于支持 exactly-once 语义），用户需要在发送数据前获取数据源的全局锁。
 */
public class SourceOperatorWithState {

  public static class CounterSource extends RichSourceFunction<Long> implements CheckpointedFunction {

    private volatile boolean isRunning = true;
    // state 变量
    private ListState<Long> offsetState;
    private Long offset;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      offsetState.clear();
      offsetState.update(Collections.singletonList(offset));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      offsetState = context.getOperatorStateStore().getListState(
          new ListStateDescriptor<>("offset state", LongSerializer.INSTANCE));
      Iterable<Long> offsetList = offsetState.get();
      // 从已保存的状态中恢复 offset 到内存中，在进行任务恢复的时候也会调用此初始化状态的方法。
      if (offsetList != null) {
        for (Long l : offsetList) {
          offset = l;
        }
      }
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
      final Object sourceLock = ctx.getCheckpointLock();
      while (isRunning) {
        // output and state update are atomic
        synchronized (sourceLock) {
          ctx.collect(offset);
          offset++;
        }
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }

}
