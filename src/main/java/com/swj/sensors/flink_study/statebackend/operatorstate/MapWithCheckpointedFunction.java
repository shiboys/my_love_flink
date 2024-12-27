package com.swj.sensors.flink_study.statebackend.operatorstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/06 15:08
 */
public class MapWithCheckpointedFunction {

  static class CustomMapFunction<T> implements CheckpointedFunction, MapFunction<T, T> {
    /**
     * 这里展示 KeyedState 的 ReducingState 和
     * OperatorState 的  ListState 的用法
     */

    private long currentItemCount;

    ReducingState<Long> totalCountPerKeyState;

    ListState<Long> countPerPartition;

    @Override
    public T map(T t) throws Exception {

      // reducingState 是每次 +1
      totalCountPerKeyState.add(1L);

      // listState 是通过变量 +1， 然后把变量存储到自己的 state 里面
      currentItemCount++;
      return t;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
      // flink 做 checkpoint 的时候调用
      countPerPartition.clear();
      countPerPartition.add(currentItemCount);
    }


    /**
     * 用户自定义函数初始化时会调用 initializeState()，初始化包括第一次自定义函数初始化和从之前的 checkpoint 恢复
     * 因此 initializeState() 不仅是定义不同状态类型初始化的地方，也需要包括状态恢复的逻辑。
     * @param functionInitializationContext
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
      //通过 checkpoint 恢复算子和状态内存储的数据

      totalCountPerKeyState = functionInitializationContext.getKeyedStateStore().getReducingState(
          new ReducingStateDescriptor<Long>("total count", new AddFunction(), Long.class));

      countPerPartition = functionInitializationContext.getOperatorStateStore().getUnionListState(
          new ListStateDescriptor<Long>("total list", Long.class));
      for (Long l : countPerPartition.get()) { // 其实 liststate 里面只有一个元素
        currentItemCount += l;
      }

      System.out.println("currentItemCount=" + currentItemCount + ", reducing count is " + totalCountPerKeyState.get());
    }
  }


  static class AddFunction implements ReduceFunction<Long> {
    @Override
    public Long reduce(Long aLong, Long t1) throws Exception {
      return aLong + t1;
    }
  }
}
