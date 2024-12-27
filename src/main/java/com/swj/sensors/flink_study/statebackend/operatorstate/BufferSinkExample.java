package com.swj.sensors.flink_study.statebackend.operatorstate;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/06 15:36
 * Flink 官方示例
 * https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/state/
 */
public class BufferSinkExample {

  // 防止 Sink 算子失败的时候造成的数据丢失
  static class BufferSinkFunction implements SinkFunction<Tuple2<String, Long>>, CheckpointedFunction {

    ListStateDescriptor<Tuple2<String, Long>> listStateDescriptor;

    private int threshold;

    List<Tuple2<String, Long>> elementsList;
    ListState<Tuple2<String, Long>> elementListState;

    public BufferSinkFunction(int threshold) {
      listStateDescriptor = new ListStateDescriptor<>("buffered elements",
          TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
          }));
      this.threshold = threshold;
      this.elementsList = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
      elementsList.add(value);
      if (elementsList.size() >= threshold) {
        for (Tuple2<String, Long> element : elementsList) {
          // send it to the sink
        }
        elementsList.clear();
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
      elementListState.clear();
      //elementListState.update();
      // 这里有点违背了 operator ListState 不宜包含过多元素的原则
      for (Tuple2<String, Long> element : elementsList) {
        elementListState.add(element);
      }
      ConfigOption<Boolean> recovery = CheckpointingOptions.LOCAL_RECOVERY;
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
      // initializeState 接收一个 FunctionInitializationContext 参数，用来初始化 non-keyed state 容器
      // 这些 state 是一个 ListState 用于在 checkpoint 的时候保存 non-keyed state 对象。
      // operator state 和 keyed state 的 descriptor 一样，都包含 名称，以及状态类型等信息。

      //调用不同的获取对象接口，会使用不同的状态分配算法。比如 getUnionListState(descriptor) 会使用 union redistribution 算法，
      // 而 getListState(descriptor) 算法则简单的使用 even-split redistribution 算法。

      elementListState = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
      // 恢复 elementList
      if (functionInitializationContext.isRestored()) {
        // 当初始化状态对象后，我们通过 isRestored() 方法判断是否从之前的故障中恢复过来。如果为 true，则接下来执行恢复逻辑。
        for (Tuple2<String, Long> element : elementListState.get()) {
          elementsList.add(element);
        }
      }
    }
  }
}
