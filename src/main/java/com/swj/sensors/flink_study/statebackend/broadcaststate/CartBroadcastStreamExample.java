package com.swj.sensors.flink_study.statebackend.broadcaststate;

import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Action;
import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Pattern;
import com.swj.sensors.flink_study.statebackend.common.KafkaExampleUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/07 21:11
 */
public class CartBroadcastStreamExample {

  public static void main(String[] args) throws Exception {
    ParameterTool tool = ParameterTool.fromArgs(args);
    String actionTopic = tool.get("action-topic", "action-topic");
    String patternTopic = tool.get("pattern-topic", "pattern-topic");

//    Properties prop = new Properties();
//    prop.put("bootstrap.servers", "localhost:9092");
//    prop.put("group.id", CartBroadcastStreamExample.class.getSimpleName());

    // 通过 KafkaExampleUtil.prepareExecutionEnv 这个静态方法，将 env 的 checkpoint 和 timeCharacteristic 设置
    StreamExecutionEnvironment env = KafkaExampleUtil.prepareExecutionEnv(tool);

    FlinkKafkaConsumer011<Action> actionConsumer =
        new FlinkKafkaConsumer011<>(actionTopic, new ActionEventSchema(),
            tool.getProperties()); // 通过 parameterTool 命令行参数后去消费者相关系想你

    FlinkKafkaConsumer011<Pattern> patternConsumer =
        new FlinkKafkaConsumer011<>(patternTopic, new PatternEventSchema(), tool.getProperties());

    KeyedStream<Action, Long> actionLongKeyedStream =
        env.addSource(actionConsumer).keyBy((KeySelector<Action, Long>) Action::getUserId);

    MapStateDescriptor<Void, Pattern> patternMapStateDescriptor = new MapStateDescriptor<>("pattern-state",
        Types.VOID, Types.POJO(Pattern.class));

    // pattern 流 写入 state
    BroadcastStream<Pattern> patternBroadcastStream =
        env.addSource(patternConsumer).broadcast(patternMapStateDescriptor);

    // 将两个流 connect 起来
    SingleOutputStreamOperator<Tuple2<Long, Pattern>> matchedStream =
        actionLongKeyedStream.connect(patternBroadcastStream).process(new ActionPatternEvaluator());

    matchedStream.print();

    env.execute(CartBroadcastStreamExample.class.getSimpleName());
  }


  private static class ActionPatternEvaluator
      extends KeyedBroadcastProcessFunction<Long, Action, Pattern, Tuple2<Long, Pattern>> {

    // 这里使用 ValueState ，是因为 这个是 keyedState, 可以直接从上下文 RuntimeContext 获取
    ValueState<String> actionValueState;

    // 而这里使用 stateDescriptor 的原因是 这个 state 只能从 broadcast 的上下文获取
    MapStateDescriptor<Void, Pattern> patternStateDescriptor;



    @Override
    public void open(Configuration parameters) throws Exception {
      actionValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("action-state", Types.STRING));

      patternStateDescriptor =
          new MapStateDescriptor<Void, Pattern>("pattern-state", Types.VOID, Types.POJO(Pattern.class));
      super.open(parameters);
    }

    @Override
    public void processElement(Action action, ReadOnlyContext ctx, Collector<Tuple2<Long, Pattern>> out)
        throws Exception {

      // get previous action from keyed state.
      String prevAction = actionValueState.value();

      // get current pattern from broadcast state
      Pattern pattern = ctx.getBroadcastState(patternStateDescriptor).get(null);

      if (pattern != null && pattern.getFirstAction().equals(prevAction) && pattern.getSecondAction().equals(
          action.getAction())) {
        // Match
        out.collect(Tuple2.of(ctx.getCurrentKey(), pattern));
      }

      actionValueState.update(action.getAction());

    }

    // This method is called for each element in the broadcast stream
    @Override
    public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<Long, Pattern>> out)
        throws Exception {
      BroadcastState<Void, Pattern> broadcastState = ctx.getBroadcastState(patternStateDescriptor);
      // 更新 broadcast state 状态的值
      // Void 类型，使用默认的 null
      broadcastState.put(null, value);
    }
  }
}
