package com.swj.sensors.flink_study.streamapi.window_related;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/27 17:24
 * 演示滑动窗口的使用，
 * 使用了自定义数据源
 */

@Slf4j
public class GroupedProcessingTimeWindowExample {

  private static final AtomicLong SINK_COUNTER = new AtomicLong();

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    // 设置 4 的并行度。尴尬，本机 4 个并行度跑步起来。不是这样原因，因为我本机是 standalone 部署的，因此不支持多并行度
    // executionEnvironment.setParallelism(2);

    DataStream<Tuple2<Long, Long>> dataStream = executionEnvironment.addSource(new DataSource());
    dataStream
        .keyBy(1)
        // 定义一个滑动窗口，窗口大小为 1.5s，每次滑动 0.5 s
        // timeWindow 是下面 SlidingEventTimeWindows.of(size, slide) 的一种快捷方式
        // 如果 slide 小于窗口大小，滑动窗口可以允许窗口重叠。这种情况下，一个元素可能会被分发到多个窗口。
        .timeWindow(Time.of(2500, TimeUnit.MILLISECONDS), Time.of(500, TimeUnit.MILLISECONDS))
        .reduce(new SumReduceFunction())

        //当然 我们也可以使用下面这几种算子; 使用 apply function 可以不用提前聚合
//        .keyBy(new FirstFieldKeySelector<Tuple2<Long,Long>, Long>())
//        .window(SlidingProcessingTimeWindows.of(Time.of(2500,TimeUnit.MILLISECONDS),
//            Time.of(500,TimeUnit.MILLISECONDS)))
//        .apply(new SumWindowFunction())
        .addSink(new SinkFunction<Tuple2<Long, Long>>() {
          @Override
          public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
            if (SINK_COUNTER.incrementAndGet() % 100 == 0) {
              log.info("skin function key is {} ,value is {}", value.f0, value.f1);
            }
          }
        });

    executionEnvironment.execute("grouped window time process");

  }

  // 自定义 key 选择器
  private static class FirstFieldKeySelector<T extends Tuple, Key> implements KeySelector<T, Key> {

    @Override
    public Key getKey(T value) throws Exception {
      return value.getField(0);
    }
  }


  // 自定义 window function
  private static class SumWindowFunction
      implements WindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long, TimeWindow> {

    @Override
    public void apply(Long key, TimeWindow window, Iterable<Tuple2<Long, Long>> input,
        Collector<Tuple2<Long, Long>> out)
        throws Exception {
      long sum = 0L;
      for (Tuple2<Long, Long> element : input) {
        sum += element.f1;
      }
      out.collect(new Tuple2<>(key, sum));
    }
  }


  // reduce function
  private static class SumReduceFunction implements ReduceFunction<Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
      return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
    }
  }



  private static class DataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
    private static final long NUM_KEYS = 10_000;
    private static final long NUM_ELEMENTS = 10_000_000;


    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
      long startTime = System.currentTimeMillis();
      long count = 0;
      long val = 1;
      while (isRunning && count < NUM_ELEMENTS) {
        ctx.collect(new Tuple2<>(val, 1L));
        val++;
        if (val > NUM_KEYS) {
          val = 1;
        }
        count++;
      }

      long timeElapsed = System.currentTimeMillis() - startTime;
      System.out.println("Took " + timeElapsed + " ms to emit " + NUM_ELEMENTS + " values");
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
