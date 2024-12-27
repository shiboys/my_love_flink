package com.swj.sensors.flink_study.statebackend.statemachine;

import com.swj.sensors.flink_study.statebackend.statemachine.dfa.State;
import com.swj.sensors.flink_study.statebackend.statemachine.event.Alert;
import com.swj.sensors.flink_study.statebackend.statemachine.event.Event;
import com.swj.sensors.flink_study.statebackend.statemachine.generataor.EventGeneratorSource;
import com.swj.sensors.flink_study.statebackend.statemachine.kafka.EventDeserializer;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/12 16:09
 * 状态机示例的主类，
 * 该主类的主要逻辑和功能是，根据用户的fink data source 参数项，选择使用 FlinkKafkaConsumer 作为数据源
 * 还是 DataGeneratorSource 作为数据源
 * 通过 flatMap 的方式，在 flatMap 里面将 Invalid Event 转化为 Alter ，
 * 通过下游算子将 Alter 展示出来，这里的 Alter 展示只是简单的打印。主要的处理逻辑在 map function 里面
 */
public class StateMachineExample {
  public static void main(String[] args) throws Exception {
    // --- print some usage help ----

    // 内置 data-generator 数据源需要配置 --error-rate 和 --sleep 参数
    System.out.println(
        "Usage with built-in data generator source: StateMachineExample [--error-rate <probability-of-invalid-transition>] [--sleep <sleep-per-record-in-mills>]");
    // 如果使用 kafka 数据源，那么只能消费 kafka 数据，异常数据产生的几率只能依靠 kafka 生产者产生的数据。
    System.out.println("Usage with kafka source: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]");

    System.out.println("options for both above 2 choices:");

    // 下面的参数选项主要有：state-backend, data.checkpoints.dir, incremental-checkpoints, async-checkpoints,output
    System.out.println("\t[--backend <rocks|file>]");
    System.out.println("\t[--checkpoint-dir <filepath>]");
    System.out.println("\t[--incremental-checkpoints <true|false>]");
    System.out.println("\t[--async-checkpoints <true|false>]");
    System.out.println("\t[--output <filepath> OR null for stdout");

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    String topic = parameterTool.get("kafka-topic");
    SourceFunction<Event> eventDataSource = null;
    if (topic != null && !topic.isEmpty()) {
      Properties properties = new Properties();
      properties.put("bootstrap.servers", parameterTool.get("brokers","localhost:9092"));
      properties.put("group.id", StateMachineExample.class.getSimpleName());

      FlinkKafkaConsumer011<Event> kafkaSource =
          new FlinkKafkaConsumer011<>(topic, new EventDeserializer(), properties);
      // 我本机运行的是 standalone flink 集群，暂时没想到怎么有 两个 task slot，就只能一个 flink 任务先发，然后停止，再起当前 flink 作业 读取接收
      kafkaSource.setStartFromGroupOffsets();
      eventDataSource = kafkaSource;
    } else {
      double errorRate = parameterTool.getDouble("error-rate", 0.0);
      long sleep = parameterTool.getLong("sleep", 1L);

      System.out.printf("Using standalone source with error rate %f and sleep delay %d mills\n", errorRate, sleep);

      eventDataSource = new EventGeneratorSource(errorRate, sleep);
    }

    // main program
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // checkpoint 设置
    env.enableCheckpointing(2000);
    String backend = parameterTool.get("backend", "memory");
    String checkpointDir = parameterTool.get("checkpoint-dir");
    if ("file".equals(backend)) {
      boolean asyncCheckpoint = parameterTool.getBoolean("async-checkpoints", false);
      env.setStateBackend(new FsStateBackend(checkpointDir, asyncCheckpoint));
    } else if ("rocks".equals(backend)) {
      boolean incrementalBackend = parameterTool.getBoolean("incremental-checkpoints", false);
      //env.setStateBackend(new RockdbStateBackend(chekpointDir,incrementalBackend));
    }
    DataStream<Alert> alertSource = env.addSource(eventDataSource).keyBy(Event::sourceAddress)
        .flatMap(new AlterMapper());
    String output = parameterTool.get("output");
    if (output == null || output.isEmpty()) {
      alertSource.print();
    } else {
      // 写入文件系统
      alertSource.writeAsText(output).setParallelism(1);
    }
    env.execute("flink state machine example");
  }

  private static class AlterMapper extends RichFlatMapFunction<Event, Alert> {

    private ValueState<State> currentState;

    @Override
    public void open(Configuration parameters) throws Exception {
      currentState = getRuntimeContext().getState(new ValueStateDescriptor<>("state", State.class));
      super.open(parameters);
    }

    @Override
    public void flatMap(Event value, Collector<Alert> out) throws Exception {
      State state = currentState.value();
      if (state == null) {
        state = State.Initial;
      }
      // 模拟触发事件，状态变成下一个状态
      State nextState = state.transition(value.type());
      if (nextState == State.InvalidTransition) { // 状态的产生是乱序的，就会导致 invalidTransition 产生
        // 此时需要产生一个 Alert
        out.collect(new Alert(value.sourceAddress(), state, value.type()));
      } else if (nextState.isTerminal()) { // 一个完整的流程在流式应用中走完了
        currentState.clear();
      } else { // 正常的流转到下一个状态
        currentState.update(nextState);
      }
    }
  }
}
