package com.swj.sensors.flink_study.statebackend.broadcaststate;

import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Color;
import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Item;
import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Rule;
import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Shape;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/08 21:22
 * 参考 flink 官方例子 https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/broadcast_state/
 * 假设存在一个序列，在序列中的有元素是具有不同颜色与形状的图形，我们希望在序列里相同颜色的图形中寻找满足一定顺序模式的图形对(比如在红色的图形中，
 * 有一个长方形跟着一个三角形)。同时，我们希望寻找的模式会随着时间而改变。
 * 在这个例子中，我们定义两个流，一个流包含图形(Item), 具有颜色和形状两个图形。另一个流包含特定的规则（Rule），大表希望寻找的模式。
 * 在图形流中，我们需要首先使用颜色进行分区（keyBy），这样能确保相同的颜色会流转到先流转到相同的物理机上。
 */
public class KeyedBroadcastProcessFunctionExample {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(1000);

    DataStream<Rule> ruleSource = env.fromCollection(getParameterRuleList());
    DataStream<Item> itemSource = env.fromCollection(getParameterManuallyItemList());

    // key the items by the color
    KeyedStream<Item, Color> colorPartitionedStream = itemSource.keyBy((KeySelector<Item, Color>) Item::getColor);

    /**
     * 对于规则流，他应该被广播到所有的下游 task 中，下游 task 应当存储这些规则并根据它来寻找满足规则的图形对。
     * 下面的示例代码会完成：
     * 1、将规则广播给所有的下游 task。
     * 2、使用 MapStateDescriptor 来描述并创建 broadcast state 在下游的存储结构。
     */
    // 一个 map descriptor ，它描述了一个用于存储规则名称与规则本身的 map 结构
    MapStateDescriptor<String, Rule> ruleMapStateDescriptor = new MapStateDescriptor<>("RuleBroadcastState",
        BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Rule>() {
    }));

    // 广播流，广播规则并且创建 broadcast state
    BroadcastStream<Rule> ruleBroadcastStream = ruleSource.broadcast(ruleMapStateDescriptor);

    /**
     * 最终，为了使用规则来筛选图形序列，我们需要：
     * 1、将两个流关联起来
     * 2、完成我们的模式识别逻辑。
     */

    /**
     * 为了关联一个非广播流(Keyed或者 non-keyed)与一个广播流(BroadcastStream), 我们可以调用非广播流的方法 connect(), 并将
     Broadcast 流当做参数传入。这个方法的返回参数是 BroadcastConnectedStream，具有类型方法 process(), 传入一个特殊的
     CoProcessFunction 来书写我们的模式识别逻辑。具体传入 process() 的是哪个类型取决于非广播的类型：
     如果是一个 keyed 流，那就是 KeyedBroadcastProcessFunction 类型。
     如果是一个 non-keyed 流，那就是 BroadcastProcessFunction 类型。
     */

    SingleOutputStreamOperator<String> output = colorPartitionedStream.connect(ruleBroadcastStream).process(
        /**
         * KeyedBroadcastProcessFunction 的类型参数解释如下：
         * 1、Keyed Stream 中的 key 的类型
         * 2、非广播流中的元素类型
         * 3、广播中的元素类型
         * 4、结果的类型，这里是 String
         */
        new KeyedBroadcastProcessFunction<Color, Item, Rule, String>() {

          //存储部分匹配的结果，即匹配了第一个元素，正在等待匹配第二个元素
          // 我们使用一个数组来存储，因为可能同时存在很多第一个元素正在等待。
          private MapStateDescriptor<String, List<Item>> mapStateDesc =
              new MapStateDescriptor<>("items",
                  BasicTypeInfo.STRING_TYPE_INFO,
                  TypeInformation.of(new TypeHint<List<Item>>() {
                  }));

          // 与之前的 ruleStateDescriptor 相同
          private MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>("RuleBroadcastState",
              BasicTypeInfo.STRING_TYPE_INFO,
              TypeInformation.of(new TypeHint<Rule>() {
              }));

          /**
           * 负责处理非广播流中的元素
           * 同时，KeyedBroadcastProcessFunction 在 Keyed Stream 上工作，所以它提供了一些 BroadcastProcessFunction 没有的功能:
           *
           * processElement() 的参数 ReadOnlyContext 提供了方法能够访问 Flink 的定时器服务，可以注册事件定时器(event-time timer)或者处理时间的定时器(processing-time timer)。
           * 当定时器触发时，会调用 onTimer() 方法， 提供了 OnTimerContext，它具有 ReadOnlyContext 的全部功能，并且提供：
           * 查询当前触发的是一个事件还是处理时间的定时器
           * 查询定时器关联的key
           * @param value
           * @param ctx
           * @param out
           * @throws Exception
           */
          @Override
          public void processElement(Item value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            Shape shape = value.getShape();
            MapState<String/*ruleName*/, List<Item>> itemListMapState = getRuntimeContext().getMapState(mapStateDesc);
            for (Map.Entry<String, Rule> entry : ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
              String ruleName = entry.getKey();
              Rule rule = entry.getValue();

              List<Item> savedItemList = itemListMapState.get(ruleName);
              if (savedItemList == null) {
                savedItemList = new ArrayList<>();
              }
              //这里按照官方示例的直接写 == 好像不行，改用 equals ，就可以了
              if (shape.equals(rule.getSecond())) {
                if (!savedItemList.isEmpty()) {
                  for (Item prevItem : savedItemList) {
                    out.collect("Item Shape Matched: color:" + ctx.getCurrentKey() + ", rule: " + ruleName +
                        ", first: " + prevItem.getShape() + ", second: " + shape);
                  }
                  savedItemList.clear();
                }
              }

              // 满足匹配规则的第一个形状
              if (shape.equals(rule.getFirst())) {
                savedItemList.add(value);
              }

              if (savedItemList.isEmpty()) {
                itemListMapState.remove(ruleName);
              } else {
                // 更新 state
                itemListMapState.put(ruleName, savedItemList);
              }
            }
          }

          /**
           * KeyedBroadcastProcessFunction 中的 processElement 和 processBroadcastElement 方法中的 context 均有一下方法：
           * 1、得到广播流的存储状态：ctx.getBroadcastState(MapStateDescriptor<K, V> stateDescriptor)
           2、查询元素的时间戳：ctx.timestamp()
           3、查询目前的Watermark：ctx.currentWatermark()
           4、目前的处理时间(processing time)：ctx.currentProcessingTime()
           5、产生旁路输出：ctx.output(OutputTag<X> outputTag, X value)
           */

          /**
           * 在 getBroadcastState() 方法中传入的 stateDescriptor 应该与调用 .broadcast(ruleStateDescriptor) 的参数相同。

           这两个方法的区别在于对 broadcast state 的访问权限不同。在处理广播流元素这端，是具有读写权限的，而对于处理非广播流元素这端是只读的。
           这样做的原因是，Flink 中是不存在跨 task 通讯的。所以为了保证 broadcast state 在所有的并发实例中是一致的，
           我们在处理广播流元素的时候给予写权限，在所有的 task 中均可以看到这些元素，并且要求对这些元素处理是一致的，
           那么最终所有 task 得到的 broadcast state 是一致的。
           */

          /**
           * 负责处理广播流中的元素。
           * processBroadcastElement() 方法中的参数 Context 会提供方法 applyToKeyedState(StateDescriptor<S, VS> stateDescriptor, KeyedStateFunction<KS, S> function)。
           * 这个方法使用一个 KeyedStateFunction 能够对 stateDescriptor 对应的 state 中所有 key 的存储状态进行某些操作
           * @param rule
           * @param ctx
           * @param out
           * @throws Exception
           */
          @Override
          public void processBroadcastElement(Rule rule, Context ctx, Collector<String> out) throws Exception {
            ctx.getBroadcastState(ruleStateDescriptor).put(rule.getName(), rule);
          }
        });

    output.print();

    env.execute("KeyedBroadcastProcessFunction");
  }

  static List<Item> getParameterManuallyItemList() {
    List<Item> items = new ArrayList<>();
    items.add(new Item(Color.RED, Shape.TRIANGLE));
    items.add(new Item(Color.RED, Shape.RECTANGLE));
    items.add(new Item(Color.RED, Shape.SQUARE));

    items.add(new Item(Color.GREEN, Shape.TRIANGLE));
    items.add(new Item(Color.GREEN, Shape.RECTANGLE));
    items.add(new Item(Color.GREEN, Shape.SQUARE));

    items.add(new Item(Color.BLUE, Shape.RECTANGLE));
    items.add(new Item(Color.BLUE, Shape.SQUARE));
    items.add(new Item(Color.BLUE, Shape.TRIANGLE));
    return items;
  }

  static List<Rule> getParameterRuleList() {
    List<Rule> ruleList = new ArrayList<>();
    ruleList.add(new Rule("stabilized", Shape.TRIANGLE, Shape.RECTANGLE));
    ruleList.add(new Rule("beautiful", Shape.RECTANGLE, Shape.SQUARE));
    ruleList.add(new Rule("ugly", Shape.SQUARE, Shape.TRIANGLE));
    return ruleList;
  }



}
