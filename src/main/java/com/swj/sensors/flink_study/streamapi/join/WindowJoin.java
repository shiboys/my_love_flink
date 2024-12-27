package com.swj.sensors.flink_study.streamapi.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/29 15:22
 * 展示 Join 的用法
 */
public class WindowJoin {
  public static void main(String[] args) throws Exception {
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    // 每秒 5 个元素
    final int rate = parameterTool.getInt("rate", 5);
    final int windowSize = parameterTool.getInt("windowSize", 2000);

    System.out.println("param rate is " + rate + ", window size is " + windowSize);
    System.out.println("you can use --rate <rate> and --windowSize <window size> to specify the options");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //Ingestion time means that the time of each individual element in the stream is determined
    // when the element enters the Flink streaming data flow.
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
    env.getConfig().setGlobalJobParameters(parameterTool);



    DataStream<Tuple2<String, Integer>> gradeSource = WindowJoinSampleData.GradeSource.dataSource(env, rate);

    DataStream<Tuple2<String, Integer>> salarySource = WindowJoinSampleData.SalarySource.dataSource(env, rate);
    // run the join program
    // 这里分拆出来只是为了好进行单元测试
    DataStream<Tuple3<String, Integer, Integer>> joinStream =
        runJoinStream(gradeSource, salarySource, windowSize);
    // 设置并行度为1，打印结果在一个线程中，而不是并行的
    joinStream.print().setParallelism(1);

    env.execute("Window Join Example");

    /**
     * 打印结果就是如下所示：
     * (john,1,7736)
     * (john,1,9671)
     * (john,1,9917)
     * (john,1,8270)
     * (john,3,9237)
     * (john,3,7736)
     * (john,3,9671)
     * (john,3,9917)
     * (john,3,8270)
     * (grace,5,9196)
     * (grace,2,9196)
     * (grace,5,9196)
     * (grace,1,9196)
     */
  }

  static DataStream<Tuple3<String, Integer, Integer>> runJoinStream(DataStream<Tuple2<String, Integer>> gradeSource,
      DataStream<Tuple2<String, Integer>> salarySource, int windowSize) {
    return gradeSource.join(salarySource)
        .where(new NameKeySelector())
        .equalTo(new NameKeySelector())
        // 千万不能忘记 window
        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
        // apple function 不能使用 lambda 表达式，否则 flink 无法推断出 tuple3 的泛型类型参数
        .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
          @Override
          public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second)
              throws Exception {
            return new Tuple3<>(first.f0, first.f1, second.f1);
          }
        });
  }

  static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {

    @Override
    public String getKey(Tuple2<String, Integer> value) throws Exception {
      return value.f0;
    }
  }
}
