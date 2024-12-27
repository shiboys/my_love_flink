package com.swj.sensors.flink_study.statebackend.savepoint;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Either;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/06 19:02
 */
public class StatefulStreamingJob {

  private static final String DEFAULT_APP_NUM = "123";


  /**
   * 带撞他IDE数据要比其他算子需要注意更多东西。
   * 为了保证更新状态以及输出的原子性（用于支持 exactly-once 语义），用户需要在发送数据前获取数据源的全局锁。
   */
  static class MySource extends RichParallelSourceFunction<Integer> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
      while (running) {
        // 还有 sourceContext.getCheckpointLock() 这个方法？？
        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.collect(1);
        }
        // 每 1 秒产生一个数组
        Thread.sleep(1000);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }


  static class MyStatefulFunction extends RichMapFunction<Integer, String>  {

    static ValueStateDescriptor<Address> AVRO_STATE_DESC = new ValueStateDescriptor<>("address-state", Address.class);

    static ValueStateDescriptor<Tuple2<String, Integer>> TUPLE_STATE_DESC = new ValueStateDescriptor<>("tuple-state",
        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

    static ValueStateDescriptor<Either<String, Boolean>> EITHER_STATE_DESC =
        new ValueStateDescriptor<>("either-state", TypeInformation.of(new TypeHint<Either<String, Boolean>>() {
        }));


    // 之所以用 transient 关键字是因为 该关键字能够阻止  jvm 对变量的的序列化
    private transient ValueState<Address> addressState;
    private transient ValueState<Tuple2<String, Integer>> tupleState;
    private transient ValueState<Either<String, Boolean>> eitherState;



    @Override
    public void open(Configuration parameters) throws Exception {
      addressState = getRuntimeContext().getState(AVRO_STATE_DESC);
      tupleState = getRuntimeContext().getState(TUPLE_STATE_DESC);
      eitherState = getRuntimeContext().getState(EITHER_STATE_DESC);
      super.open(parameters);
    }

    @Override
    public String map(Integer aInt) throws Exception {
      touchState(tupleState, () -> Tuple2.of("100", 100));
      touchState(eitherState, () -> Either.Left("flink"));

      Address newAddress = Address.newBuilder()
          .setCity("BeiJing")
          .setNum(120)
          .setState("BeiJing")
          .setStreet("ChangAnJie")
          .setZip("110110")
          .build();

      Address oldAddress = addressState.value();
      if (oldAddress != null) {
        if (!Objects.equals(oldAddress.getAppno(), DEFAULT_APP_NUM)) {
          // we should throw exception here
          System.out.println("Incorrect App Number");
        }
      }
      addressState.update(newAddress);
      return "";
    }

    static <T> void touchState(ValueState<T> state, Supplier<T> supplier) {
      try {
        T val = state.value();
        if (val == null) {
          val = supplier.get();
        }
        state.update(val);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }


  public static void main(String[] args) {
    final ParameterTool tool = ParameterTool.fromArgs(args);

    final String checkpointDir = tool.getRequired("checkpoint.dir");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.enableCheckpointing(1000);

    env.setStateBackend(new FsStateBackend(checkpointDir));

    // 不通这个 api
    env.setRestartStrategy(RestartStrategies.noRestart());


    // 用 Kryo for serialization 的类型，将会抛出异常
    env.getConfig().disableGenericTypes();

    // 给各个算子加上 uid 有利于从 savepoint 恢复
    //如果不手动指定 id,则会自动生成 Id。只要这些 Id 不变，就可以从 SavePoint 中自动恢复。
    // 自动生成的 Id 取决于程序的结构，并且对程序的更改很敏感。因此强烈建议手动分配这些Id。
    // 有 id 的算子，如果从 savepoint 恢复，就算顺序更改也没关系
    env.addSource(new MySource()).uid("my-source")
        .keyBy(anyInt->0L)
        .map(new MyStatefulFunction()).uid("my-map")
        .addSink(new DiscardingSink<>()).uid("my-sink");
  }
  /**
   *
   */
}
