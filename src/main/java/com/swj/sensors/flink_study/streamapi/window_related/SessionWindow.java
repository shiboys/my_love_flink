package com.swj.sensors.flink_study.streamapi.window_related;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/28 14:54
 * Session Window 测试
 */
public class SessionWindow {
  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    executionEnvironment.getConfig().setGlobalJobParameters(parameterTool);
    // 设置 流处理的时间类型为 时间类型
    executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    List<Tuple3<String, Long, Integer>> list = new ArrayList<>();
    list.add(new Tuple3<>("a", 1L, 1));
    list.add(new Tuple3<>("b", 1L, 1));
    list.add(new Tuple3<>("b", 3L, 1));
    list.add(new Tuple3<>("b", 5L, 1));
    list.add(new Tuple3<>("c", 6L, 1));
    // 如果我们的 session window gap 设置 3 ，设置 tuple 的 1 索引位置 为 timestamp 位置，那么 6L 到 11L 之间的时间差为 5L
    // 会导致就会导致 1 个 session window 结束 ，此时如果想要将 这个已经结束的 session window emit 出去，就要看
    // 此时的 watermark 是否比 window 结束的时间大，如果我们设置 watermark 为 tuple3.f1 -1 ，那么满足 watermark 的时间比上个 session
    // window 的结束时间大，就可以触发上一个 session window
    list.add(new Tuple3<>("a", 10L, 1));
    list.add(new Tuple3<>("c", 11L, 1));

    DataStream<Tuple3<String, Long, Integer>> streamSource =
        executionEnvironment.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
          @Override
          public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
            for (Tuple3<String, Long, Integer> tuple : list) {
              // 告诉下游算子，我的 timestamp 为 tuple1.f1
              ctx.collectWithTimestamp(tuple, tuple.f1);
              // 发射一个  tuple.f1-1 的 watermark
              ctx.emitWatermark(new Watermark(tuple.f1 - 1));
            }

            // 最后还要发射一个比较大的 watermark ，要将 当前 window 中的元素全部 emmit 出去
            ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
          }

          @Override
          public void cancel() {

          }
        });

    DataStream<Tuple3<String, Long, Integer>> sumResult = streamSource
        .keyBy(0)
        .window(EventTimeSessionWindows.withGap(Time.milliseconds(3)))
        .sum(2);

    if (parameterTool.has("output")) {
      sumResult.writeAsText(parameterTool.get("output"));
    } else {
      System.out.println("print result to stdout. you can use output option to specify the result path");
      sumResult.print();
    }

    executionEnvironment.execute("session window test");
    /**
     * 打印结果如下：
     * (a,1,1)
     * (b,1,3)
     * (c,6,1)
     * (a,10,1)
     * (c,11,1)
     * 可以看到 b 在第一个 session window 中是 3 个，timestamp 是 最开始的 1
     * a在遇到第二个 a 的时候，因为此时已经是第二个 session window 了，所以开始了一个新的窗口计算，所以两个 a 是独立打印的，b 也是同理
     */

  }
}
