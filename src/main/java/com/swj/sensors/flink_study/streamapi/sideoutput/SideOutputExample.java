package com.swj.sensors.flink_study.streamapi.sideoutput;

import com.swj.sensors.flink_study.streamapi.wordcount.util.WordCountData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/29 17:45
 * 旁路输出演示
 */
public class SideOutputExample {

  static final OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {
  };

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String input = parameterTool.get("input");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(parameterTool);
    // stream 的 window 窗口用到了 EventTime, 所以这里必须设置为 IngestionTime 或者
    // 自定义 EventTime Extractor
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

    DataStream<String> textStream;

    if (parameterTool.has("input")) {
      textStream = env.readTextFile(input);
    } else {
      textStream = env.fromElements(WordCountData.WORDS);
    }
    // 所有的 key 都归到一个分组里面
    SingleOutputStreamOperator<Tuple2<String, Integer>> tokenizer = textStream
        .keyBy(new KeySelector<String, Integer>() {
          @Override
          public Integer getKey(String value) throws Exception {
            return 0;
          }
        })
        .process(new Tokenizer());

    // 将 reject 标签的数据重新 map 成 string.format
    DataStream<String> rejectedStream =
        tokenizer
            .getSideOutput(rejectedWordsTag)
            .map(new MapFunction<String, String>() {
              @Override
              public String map(String value) throws Exception {
                return String.format("rejected: %s", value);
              }
            });

    DataStream<Tuple2<String, Integer>> sumResult =
        tokenizer
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .sum(1);

    // emit result
    if (parameterTool.has("output")) {
      sumResult.writeAsText(parameterTool.get("output"));
      rejectedStream.writeAsText(parameterTool.get("reject-words-output"));
    } else {
      System.out.println("write result to stdout and you can get the result by specifying the output option");
      sumResult.print();
      rejectedStream.print();
    }

    env.execute("Side Output Example");
    /**
     * 运行结果：
     * rejected: resolution
     * rejected: sicklied
     * rejected: thought
     * rejected: enterprises
     * rejected: moment
     * rejected: regard
     * rejected: currents
     * rejected: action
     * rejected: Ophelia
     * rejected: orisons
     * rejected: remember
     *
     * 发送到下游的单词
     * (coil,1)
     * (this,2)
     * (off,1)
     * (have,2)
     * (When,2)
     */
  }

  private static class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {

    @Override
    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
      String[] tokenArr = value.split("\\W+");
      for (String token : tokenArr) {
        if (token.length() > 5) {
          // 将 value 打上 reject 标签，并且不发送到下游算子
          ctx.output(rejectedWordsTag, token);
        } else if (value.length() > 0) {
          out.collect(new Tuple2<>(token, 1));
        }
      }
    }
  }
}
