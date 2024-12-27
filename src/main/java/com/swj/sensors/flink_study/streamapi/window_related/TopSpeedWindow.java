package com.swj.sensors.flink_study.streamapi.window_related;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/28 16:56
 * Global Window 应用示例：
 * 在该示例中，还应用了 Evictor 和自定义的 delta trigger
 */
public class TopSpeedWindow {
  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(parameterTool);
    DataStream<Tuple4<Integer, Integer, Double, Long>> dataSource;
    if (parameterTool.has("input")) {
      dataSource = env.readTextFile(parameterTool.get("input")).map(new ParseCarData());
    } else {
      dataSource = env.addSource(CarSource.create(2));
    }
    int evictSeconds = 10;
    double triggerMeters = 50;
    DataStream<Tuple4<Integer, Integer, Double, Long>> speedResult = dataSource
        .assignTimestampsAndWatermarks(new CarTimestampExtractor())
        .keyBy(0)
        .window(GlobalWindows.create()) // 使用 globalWindow
        // 由于使用了 globalWindow, 因此窗口内的元素，flink 就不再帮我们清除, 需要我们自己手动清除，这里就用到了 evictor 函数。
        // timeEvicotor 接收 interval 参数，以毫秒表示，会找到窗口中最大的 time max_ts, 然后将 timestamp 小于 max_ts-interval
        // 的所有元素在发送给 window function 之前删除。
        .evictor(TimeEvictor.of(Time.of(evictSeconds, TimeUnit.SECONDS)))
        // 没有了事件和数量这种窗口，是一种全局的窗口，怎么触发只能你自己说了算，因此这里需要增加自定触发逻辑
        // 这里使用了比较简单的 DeltaTrigger. DeltaTrigger 使用了当前数据点和上一次触发时的数据增量来跟 threshold 比较
        // 大于 threshold 则代表可以触发。也即是说 上次和这次超过 50 米，就可以将此时的 tuple 发出来了。
        .trigger(DeltaTrigger.of(triggerMeters,
            (DeltaFunction<Tuple4<Integer, Integer, Double, Long>>) (oldDataPoint, newDataPoint) -> newDataPoint.f2
                - oldDataPoint.f2
            , dataSource.getType().createSerializer(env.getConfig())))
        // 按第二个字段取最大的
        .maxBy(1);

    if (parameterTool.has("output")) {
      speedResult.writeAsText(parameterTool.get("output"));
    } else {
      System.out.println("write job result to stdout, you can write to a file path via the --output option.");
      speedResult.print();
    }

    env.execute("Top Speed Window");
    /**
     * 看了 global 的打印总结，感觉一团浆糊。bad idea
     */
  }

  private static class ParseCarData implements MapFunction<String, Tuple4<Integer, Integer, Double, Long>> {

    @Override
    public Tuple4<Integer, Integer, Double, Long> map(String value) throws Exception {
      String input = value.substring(1, value.length() - 1);
      // 格式为 (0,55,15.277777777777777,1424951918630)
      String[] arr = input.split(",");
      return new Tuple4<>(Integer.valueOf(arr[0]), Integer.valueOf(arr[1]), Double.valueOf(arr[2]),
          Long.valueOf(arr[3]));
    }
  }


  private static class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {


    private volatile boolean isRunning = true;
    private final int[] speed;
    private final double[] distance;
    private final Random random = new Random();

    private CarSource(int count) {
      if (count < 1)
        throw new IllegalArgumentException("count is less than one");
      speed = new int[count];
      distance = new double[count];

      Arrays.fill(speed, 50);
      Arrays.fill(distance, 0d);

    }

    public static CarSource create(int carCount) {
      return new CarSource(carCount);
    }

    @Override
    public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx) throws Exception {
      // 每 100 ms 发出一条数据
      while (isRunning) {
        Thread.sleep(100);
        for (int carId = 0; carId < speed.length; carId++) {
          if (random.nextBoolean()) {
            speed[carId] = Math.min(100, speed[carId] + 5);
          } else {
            speed[carId] = Math.max(0, speed[carId] - 5);
          }
          // 除以 3.6 的原因是 速度一般是以 时速 多少公里计算。1 km = 1000m, 1 小时 = 3600 秒，1000m/3600s = 可以得出每秒形式多少米
          // 这里是累加的，表示行进的举例
          distance[carId] += speed[carId] / 3.6d;
          Tuple4<Integer, Integer, Double, Long> tuple4 =
              new Tuple4<>(carId, speed[carId], distance[carId], System.currentTimeMillis());
          ctx.collect(tuple4);
        }
      }

    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }


  private static class CarTimestampExtractor
      extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {

    @Override
    public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
      // 返回一个递增的 ts，flink 会用这个 ts 跟它 记录的 ts 比较，选择一个较大的 ts 做为当前 ts
      return element.f3;
    }
  }
}
